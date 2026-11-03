"""
The heterodyne pipeline: ``pipeline: cwinpy-heterodyne``.

One analysis is one *shard* of an observing run: a span of time, for some detectors,
heterodyned for every pulsar of the run in a single HTCondor DAG made by cwinpy's
``HeterodyneDAGRunner``. The shards of a run belong to a "run" subject, and the
``cwinpy-merge`` analysis of that subject needs all of them.

cwinpy accepts its settings in many forms. This takes them from the ledger and
hands cwinpy one canonical form, an explicit dictionary for each detector, so that
none of its other code paths is exercised.

What is known about how cwinpy and HTCondor behave, and was seen by running them:

* cwinpy writes an empty file for each output when it builds the DAG, so a file
  that exists is not a result. An output is complete when it is not empty.
* With cwinpy's default settings a job which cannot read a frame file exits
  successfully and writes nothing, so data is lost without a failure. Hence
  ``strict data requirement`` is true here, which makes the job fail, and DAGMan
  can then be asked to run only the jobs that failed.
* DAGMan resumes from a rescue file when the same DAG is submitted again.
"""

import configparser
import contextlib
import importlib.resources
import inspect
import io
import json
import os
import re

from asimov import config
from asimov.pipeline import PipelineException
from asimov.utils import set_directory

from .common import CwinpyError, CwinpyPipeline, TransientCwinpyError

#: The name cwinpy gives the DAG it writes.
DAG_NAME = "cwinpy_heterodyne.dag"

#: What this pipeline writes in the run directory, and cwinpy writes in the DAG
#: directory, to say what to expect.
MANIFEST = "manifest.json"

#: The asset: ``{pulsar: {detector: {factor: [path, ...]}}}``, once complete.
ASSET = "heterodyned files"

#: How DAGMan says in its log that it has finished, and how.
_EXITED = re.compile(r"EXITING WITH STATUS (\d+)")

#: What a job writes to its error output when it could not read a frame file,
#: found by making cwinpy read a damaged one. These are the failures that trying
#: again is likely to fix, as a disk or a machine comes back.
FRAME_READ_ERRORS = (
    r"Failed to read frame data",
    r"XLALFrFileOpen",
    r"XLALFrStream",
    r"Could not open TOC",
    r"I/O error",
    r"Stale file handle",
)

#: How much of the end of the DAGMan log is read, which is where it ends.
_TAIL = 100_000


class HeterodynePipeline(CwinpyPipeline):
    """Heterodyne one shard of an observing run, for all of its pulsars."""

    name = "cwinpy-heterodyne"
    pipeline_name = "cwinpy-heterodyne"
    step = "4"

    available_outputs = [ASSET, "config"]
    declares_inputs = True

    #: The DAGMan log is what to report, not the logs of thousands of jobs.
    log_patterns = ["submit/*.dagman.out"]

    #: Ledger keys that the configuration needs and that have no safe default.
    REQUIRED_META = (
        ("interferometers",),
        ("cwinpy-heterodyne", "data start"),
        ("cwinpy-heterodyne", "data end"),
        ("cwinpy-heterodyne", "ephemerides"),
        ("data", "channels"),
    )

    # What the analysis says

    def settings(self):
        """The ``cwinpy-heterodyne:`` block of the analysis."""
        return self.production.meta.get("cwinpy-heterodyne") or {}

    def data(self):
        """The ``data:`` block of the analysis."""
        return self.production.meta.get("data") or {}

    def detectors(self):
        return list(self.production.meta["interferometers"])

    def factors(self):
        """
        The factors of the rotation frequency to heterodyne at.

        Raises
        ------
        PipelineException
            If a factor is not a positive number.
        """
        found = self.production.meta.get("frequency factors") or [2]
        factors = list(found) if isinstance(found, (list, tuple)) else [found]
        for factor in factors:
            if isinstance(factor, bool) or not isinstance(factor, (int, float)) or factor <= 0:
                raise PipelineException(
                    f"`frequency factors` must be positive numbers, not {factor!r}.",
                    production=self.production.name,
                )
        return factors

    def ephemerides(self):
        """The parameter files of the pulsars, as a list of paths."""
        found = self.settings().get("ephemerides")
        return [found] if isinstance(found, str) else list(found or [])

    def times(self):
        """``(start, end)``, in GPS seconds."""
        try:
            start = int(self.settings()["data start"])
            end = int(self.settings()["data end"])
        except (KeyError, TypeError, ValueError) as error:
            raise PipelineException(
                "`data start` and `data end` must be GPS times.", production=self.production.name
            ) from error
        if end <= start:
            raise PipelineException(
                f"`data end` ({end}) must be after `data start` ({start}).",
                production=self.production.name,
            )
        return start, end

    def per_detector(self, term, source):
        """
        A setting that has a value for each detector.

        Raises
        ------
        PipelineException
            If a detector has none.
        """
        found = source.get(term) or {}
        missing = [d for d in self.detectors() if d not in found]
        if missing:
            raise PipelineException(
                f"There is no `{term}` for {', '.join(missing)}.", production=self.production.name
            )
        return {d: found[d] for d in self.detectors()}

    def from_caches(self):
        """Whether the data is in frame cache files, and not found on a datafind server."""
        return bool(self.data().get("cache files"))

    # The configuration of cwinpy_heterodyne_pipeline

    def results_directory(self, detector=None):
        base = os.path.join(self.production.rundir, "results")
        return base if detector is None else os.path.join(base, detector)

    def runner_config(self):
        """
        The configuration for cwinpy's ``HeterodyneDAGRunner``, in its canonical form.

        Every setting that cwinpy accepts in several forms is given in one: a dictionary
        with a value for each detector, written as a Python literal.

        Returns
        -------
        configparser.ConfigParser

        Raises
        ------
        PipelineException
            If the ledger lacks something the data needs.
        """
        parser = configparser.ConfigParser(interpolation=None)
        parser["run"] = {"basedir": self.production.rundir}
        parser["heterodyne_dag"] = self._dag_section()
        parser["heterodyne"] = self._heterodyne_section()
        parser["ephemerides"] = {"pulsarfiles": repr(self.ephemerides())}
        # The merge is its own analysis, which needs all the shards.
        parser["merge"] = {"merge": "False"}
        return parser

    def _dag_section(self):
        """How the DAG is made and what its jobs ask for."""
        asked = self.production.meta.get("scheduler") or {}
        section = {
            "build": "True",
            "submitdag": "False",
            "submit": os.path.join(self.production.rundir, "submit"),
        }
        for key, option in (
            ("request memory", "request_memory"),
            ("request disk", "request_disk"),
            ("request cpus", "request_cpus"),
            ("accounting group", "accounting_group"),
        ):
            if asked.get(key) is not None:
                section[option] = str(asked[key])
        if asked.get("accounting group"):
            try:
                section["accounting_group_user"] = config.get("condor", "user")
            except Exception:
                pass
        if self.data().get("datafind url type"):
            section["urltype"] = str(self.data()["datafind url type"])
        return section

    def _heterodyne_section(self):
        """What is heterodyned: when, for which detectors, and where the data is."""
        settings, data = self.settings(), self.data()
        detectors = self.detectors()
        start, end = self.times()
        section = {
            "detectors": repr(detectors),
            "starttimes": repr({d: start for d in detectors}),
            "endtimes": repr({d: end for d in detectors}),
            "freqfactors": repr(self.factors()),
            "channels": repr(self.per_detector("channels", data)),
            "outputdir": repr({d: self.results_directory(d) for d in detectors}),
            "strictdatarequirement": str(settings.get("strict data requirement", True)),
        }
        if settings.get("joblength") is not None:
            section["joblength"] = str(int(settings["joblength"]))
        if self.from_caches():
            section["framecaches"] = repr(self.per_detector("cache files", data))
            section["segmentlists"] = repr(self.per_detector("segment lists", settings))
        else:
            section["frametypes"] = repr(self.per_detector("frame types", data))
            section["includeflags"] = repr(self.per_detector("segments", data))
            if data.get("datafind url"):
                section["host"] = str(data["datafind url"])
            if settings.get("segment server"):
                section["segmentserver"] = str(settings["segment server"])
        return section

    def render_config(self):
        """The text of the configuration for ``cwinpy_heterodyne_pipeline``."""
        text = io.StringIO()
        self.runner_config().write(text)
        return text.getvalue().rstrip("\n")

    @property
    def config_template(self):
        """The bundled template that the configuration is made from."""
        return str(
            importlib.resources.files("asimov_cwinpy").joinpath("configs/cwinpy-heterodyne.ini")
        )

    def before_config(self, dryrun=False):
        """
        Check that the ledger supplies everything that the configuration needs.

        Raises
        ------
        PipelineException
            If a value is missing, a parameter file does not exist, or the data cannot
            be found as it is described.
        """
        missing = []
        for path in self.REQUIRED_META:
            value = self.production.meta
            for key in path:
                value = value.get(key) if isinstance(value, dict) else None
            if value in (None, "", [], {}):
                missing.append(": ".join(path))
        if missing:
            raise PipelineException(
                f"The ledger does not set {', '.join(missing)} for this analysis.",
                production=self.production.name,
            )
        absent = [path for path in self.ephemerides() if not os.path.isfile(path)]
        if absent:
            raise PipelineException(
                f"{len(absent)} parameter file(s) of `ephemerides` do not exist, "
                f"for example {absent[0]}.",
                production=self.production.name,
            )
        self.runner_config()
        super().before_config(dryrun=dryrun)

    # Building

    def dag_file(self):
        return os.path.join(self.production.rundir, "submit", DAG_NAME)

    def manifest_path(self):
        return os.path.join(self.production.rundir, MANIFEST)

    @contextlib.contextmanager
    def _environment(self):
        """
        The environment that cwinpy is built in: the pipelines' environment first on
        the path, because cwinpy finds the executable of the jobs there.
        """
        before = os.environ.get("PATH", "")
        try:
            binaries = os.path.join(config.get("pipelines", "environment"), "bin")
        except Exception:
            binaries = None
        if binaries and os.path.isdir(binaries):
            os.environ["PATH"] = binaries + os.pathsep + before
        try:
            yield
        finally:
            os.environ["PATH"] = before

    def _config_file(self):
        """The configuration that asimov has made, from the event repository."""
        repository = self.production.event.repository
        if not repository:
            return f"{self.production.name}.ini"
        found = repository.find_prods(self.production.name, self.category)
        if not found:
            raise PipelineException(
                f"No configuration file found for {self.production.name} in the event "
                f"repository's '{self.category}' directory.",
                production=self.production.name,
            )
        return os.path.join(repository.directory, self.category, found[0])

    def build_dag(self, dryrun=False):
        """
        Make the DAG with cwinpy, and write down which files it will produce.

        If it has been made already, it is not made again: submitting a DAG again is
        how it is resumed, and making it again would lose the rescue file.

        Returns
        -------
        str
            The path of the DAG file.
        """
        rundir = self.production.rundir
        if rundir is None:
            raise PipelineException(
                "This analysis has no run directory: set `working directory` on the subject "
                "(or `rundir` on the analysis) in the ledger.",
                production=self.production.name,
            )
        config_file = self._config_file()
        if dryrun:
            print(f"Configuration file: {config_file}")
            return self.dag_file()
        if os.path.exists(self.manifest_path()) and os.path.exists(self.dag_file()):
            return self.dag_file()

        os.makedirs(rundir, exist_ok=True)
        parser = configparser.ConfigParser(interpolation=None)
        with open(config_file) as handle:
            parser.read_file(handle)
        try:
            from cwinpy.heterodyne.heterodyne import HeterodyneDAGRunner
        except ImportError as error:
            raise PipelineException(
                "cwinpy must be installed where the DAG is built.", production=self.production.name
            ) from error
        try:
            with self._environment():
                runner = HeterodyneDAGRunner(parser)
        except Exception as error:
            raise PipelineException(
                f"cwinpy could not make the DAG: {error}", production=self.production.name
            ) from error

        records = [
            {"detector": det, "factor": factor, "pulsar": pulsar, "files": list(files)}
            for det, by_factor in runner.heterodyned_files.items()
            for factor, by_pulsar in by_factor.items()
            for pulsar, files in by_pulsar.items()
        ]
        with open(self.manifest_path(), "w") as handle:
            json.dump({"dag": str(runner.dag_file), "outputs": records}, handle, indent=1)
        return str(runner.dag_file)

    # What it should produce

    def manifest(self):
        """
        What the build recorded.

        Returns
        -------
        dict or None
            ``{"dag": path, "outputs": [{detector, factor, pulsar, files}, ...]}``, or None
            if it has not been built.
        """
        try:
            with open(self.manifest_path()) as handle:
                return json.load(handle)
        except (OSError, ValueError):
            return None

    def expected_files(self):
        """Every file that the DAG should produce."""
        built = self.manifest()
        return [f for record in built["outputs"] for f in record["files"]] if built else []

    def incomplete_outputs(self):
        """The expected files that are missing or empty."""
        found = []
        for path in self.expected_files():
            try:
                if os.path.getsize(path) == 0:
                    found.append(path)
            except OSError:
                found.append(path)
        return found

    # Submitting

    def _has_run(self):
        """Whether the DAG has been submitted before, so that DAGMan left its files."""
        dag = self.dag_file()
        return any(os.path.exists(f"{dag}{ending}") for ending in (".condor.sub", ".dagman.out"))

    def _rotate_dagman_log(self):
        """
        Keep the log of the last run of the DAG under another name, so that the end of
        the previous run is not mistaken for the end of the next.
        """
        log = f"{self.dag_file()}.dagman.out"
        if not os.path.exists(log):
            return
        number = 1
        while os.path.exists(f"{log}.{number}"):
            number += 1
        os.rename(log, f"{log}.{number}")

    def submit_dag(self, dryrun=False):
        """
        Submit the DAG to the scheduler, or, if it has run before, submit it again so
        that DAGMan resumes from its rescue file and runs only what failed.

        Returns
        -------
        int
            The identifier that the scheduler gave the DAG.

        Raises
        ------
        PipelineException
            If the DAG cannot be made or submitted, or has run before and the scheduler
            cannot submit it again (asimov before #314).
        """
        dag = self.build_dag(dryrun=dryrun)
        batch = f"cwinpy-heterodyne/{self.production.event.name}/{self.production.name}"
        if dryrun:
            print(f"Would submit: {dag} as {batch}")
            return None

        again = self._has_run()
        arguments = {}
        if again:
            if "force" not in inspect.signature(self.scheduler.submit_dag).parameters:
                raise PipelineException(
                    "The scheduler cannot submit a DAG again, which is how a DAG that failed in "
                    "part is resumed. That needs a version of asimov with etive-io/asimov#314.",
                    production=self.production.name,
                )
            arguments["force"] = True
            self._rotate_dagman_log()
            self.logger.info("Submitting the DAG again, to resume from its rescue file")
        with set_directory(os.path.dirname(dag)):
            try:
                cluster = self.scheduler.submit_dag(dag, batch_name=batch, **arguments)
            except (FileNotFoundError, RuntimeError, NotImplementedError) as error:
                raise PipelineException(
                    f"The DAG could not be submitted: {error}", production=self.production.name
                ) from error
        self.production.status = "running"
        self.production.job_id = int(cluster)
        self.logger.info(f"Submitted DAG: {self.production.job_id}")
        return int(cluster)

    # Knowing how it went

    def dag_outcome(self):
        """
        How the last run of the DAG ended, from the log of DAGMan.

        Returns
        -------
        str or None
            ``"succeeded"`` or ``"failed"`` (the DAG ended, with or without a rescue file
            for the nodes that failed), or None if there is no sign that it has ended.
        """
        try:
            with open(f"{self.dag_file()}.dagman.out", "rb") as handle:
                handle.seek(0, os.SEEK_END)
                handle.seek(max(0, handle.tell() - _TAIL))
                text = handle.read().decode("utf-8", errors="replace")
        except OSError:
            return None
        ended = _EXITED.findall(text)
        if not ended:
            return None
        return "succeeded" if int(ended[-1]) == 0 else "failed"

    def detect_completion(self):
        """
        Whether the DAG succeeded and every output has data in it.

        Not whether the files exist: cwinpy makes an empty one for each when it builds the
        DAG. Nothing is read until DAGMan says it has succeeded, so this is cheap to ask
        every time the monitor runs.
        """
        if not self.production.rundir or self.manifest() is None:
            return False
        return self.dag_outcome() == "succeeded" and not self.incomplete_outputs()

    def failed_nodes(self):
        """
        The error output of the jobs that reported a frame-read failure.

        Returns
        -------
        list of str
            The paths of the files.
        """
        pattern = re.compile("|".join(FRAME_READ_ERRORS))
        directory = os.path.join(self.production.rundir, "log")
        found = []
        try:
            names = sorted(os.listdir(directory))
        except OSError:
            return found
        for name in names:
            if not name.endswith(".err"):
                continue
            path = os.path.join(directory, name)
            try:
                with open(path, errors="replace") as handle:
                    if pattern.search(handle.read()):
                        found.append(path)
            except OSError:
                continue
        return found

    def resurrect(self):
        """
        The DAG has left the scheduler and is not complete.

        That is not always a failure: the list of jobs that asimov has may be out of date.
        So this raises only when DAGMan says the DAG ended badly, or ended well with outputs
        that are empty; and otherwise leaves it alone and is asked again.

        A failure in which jobs could not read frame files is transient: a disk or a machine
        comes back, and asimov's retry policy decides how many times, and how long after, to
        submit the DAG again. DAGMan then runs only the jobs that failed.

        Raises
        ------
        TransientCwinpyError
            If jobs could not read frame files.
        CwinpyError
            If the DAG failed for another reason, or succeeded with empty outputs.
        """
        outcome = self.dag_outcome()
        if outcome == "failed":
            unreadable = self.failed_nodes()
            if unreadable:
                raise TransientCwinpyError(
                    f"{len(unreadable)} job(s) of {self.production.name} could not read frame "
                    f"files, for example {os.path.basename(unreadable[0])}.",
                    production=self.production.name,
                )
            raise CwinpyError(
                f"The DAG of {self.production.name} ended with failed jobs; see the logs in "
                f"{os.path.join(self.production.rundir, 'log')}.",
                production=self.production.name,
            )
        if outcome == "succeeded":
            incomplete = self.incomplete_outputs()
            if incomplete:
                raise CwinpyError(
                    f"The DAG of {self.production.name} succeeded but {len(incomplete)} of its "
                    f"outputs are missing or empty, for example {incomplete[0]}. Data was not "
                    f"read: check that `strict data requirement` is not false.",
                    production=self.production.name,
                )
            return
        self.logger.debug("The DAG has gone, but nothing says that it ended; leaving it")

    def collect_assets(self):
        """
        What the shard has made.

        Returns
        -------
        dict
            ``heterodyned files``, ``{pulsar: {detector: {factor: [path, ...]}}}``, once
            the shard is complete.
        """
        if not self.detect_completion():
            return {}
        files = {}
        for record in self.manifest()["outputs"]:
            by_detector = files.setdefault(record["pulsar"], {})
            by_detector.setdefault(record["detector"], {})[record["factor"]] = record["files"]
        return {ASSET: files}

    def output_paths(self):
        return {ASSET: self.expected_files()}
