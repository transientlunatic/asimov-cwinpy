"""
The parameter estimation pipeline: ``pipeline: cwinpy-pe``.

One analysis is one pulsar: a single ``cwinpy_pe`` job. It reads that pulsar's slice
of the ``heterodyned data`` asset of the analysis it needs (the ``cwinpy-merge`` of
the run), or simulates noise if the analysis says ``fake asd``.
"""

import importlib.resources
import os
import re
import shutil

from asimov import config
from asimov.pipeline import PipelineException
from asimov.scheduler_utils import create_job_from_dict
from asimov.utils import set_directory

from .common import CwinpyError, CwinpyPipeline
from .priors import CwinpyPriorInterface

#: The asset of ``cwinpy-merge``: ``{pulsar: {detector: {factor: path}}}``.
DATA_ASSET = "heterodyned data"

#: The options of ``cwinpy_pe`` for the data (and the simulated noise) at each
#: factor of the rotation frequency that it can analyse.
DATA_OPTIONS = {1: "data-file-1f", 2: "data-file-2f"}
FAKE_OPTIONS = {1: "fake-asd-1f", 2: "fake-asd-2f"}

#: What ``cwinpy_pe`` does when the analysis does not say.
DEFAULT_FACTORS = (2,)

#: The exit code with which ``cwinpy_pe`` stops itself, to be started again.
RESTART_CODE = 77

#: How HTCondor records, in the log of a job, that it has been submitted (000), has
#: ended (005) and how, or has been removed (009). Each event ends with a line "...".
_EVENT = re.compile(r"\s*(\d{3}) \(")
_EXITED = re.compile(r"Normal termination \(return value (\d+)\)")
_KILLED = re.compile(r"Abnormal termination \(signal (\d+)\)")

#: What Python writes before it ends a program with an exception.
_TRACEBACK = "Traceback (most recent call last)"


class PEPipeline(CwinpyPipeline):
    """Estimate the parameters of the signal of one pulsar."""

    name = "cwinpy-pe"
    pipeline_name = "cwinpy-pe"
    step = "3"

    available_outputs = ["samples", "config"]
    declares_inputs = True

    #: Ledger keys that the configuration needs and that have no safe default.
    REQUIRED_META = (("ephemeris",), ("interferometers",), ("priors",))

    # What the analysis says

    def settings(self):
        """The ``cwinpy-pe:`` block of the analysis."""
        return self.production.meta.get("cwinpy-pe") or {}

    def pulsar(self):
        """The name of the pulsar, which is the name of the subject."""
        return self.production.event.name

    def interferometers(self):
        return list(self.production.meta["interferometers"])

    def fake(self):
        """Whether the data is simulated."""
        return bool(self.settings().get("fake asd"))

    def factors(self):
        """
        The factors of the rotation frequency to analyse, as integers.

        Raises
        ------
        PipelineException
            If a factor is one that ``cwinpy_pe`` has no option for.
        """
        found = self.production.meta.get("frequency factors") or list(DEFAULT_FACTORS)
        factors = []
        for factor in found if isinstance(found, (list, tuple)) else [found]:
            if float(factor) not in {float(f) for f in DATA_OPTIONS}:
                raise PipelineException(
                    f"cwinpy-pe can analyse the frequency factors {sorted(DATA_OPTIONS)}, "
                    f"not {factor}.",
                    production=self.production.name,
                )
            factors.append(int(float(factor)))
        return factors

    # What it reads

    def get_actual_inputs(self, production):
        """It needs the heterodyned data, unless the data is simulated."""
        return [] if self.fake() else [DATA_ASSET]

    def data_files(self):
        """
        The heterodyned data of this pulsar.

        Returns
        -------
        dict
            ``{factor: {detector: path}}`` for the factors and detectors of the analysis.

        Raises
        ------
        PipelineException
            If an analysis it needs does not provide a file that is needed.
        """
        provided = self.production._previous_assets().get(DATA_ASSET) or {}
        mine = provided.get(self.pulsar())
        if mine is None:
            raise PipelineException(
                f"No analysis that {self.production.name} needs provides {DATA_ASSET} "
                f"for {self.pulsar()}.",
                production=self.production.name,
            )
        files = {}
        missing = []
        for factor in self.factors():
            files[factor] = {}
            for detector in self.interferometers():
                by_factor = {float(key): path for key, path in (mine.get(detector) or {}).items()}
                path = by_factor.get(float(factor))
                if path is None:
                    missing.append(f"{detector} at {factor}f")
                else:
                    files[factor][detector] = path
        if missing:
            raise PipelineException(
                f"The {DATA_ASSET} for {self.pulsar()} lacks {', '.join(missing)}.",
                production=self.production.name,
            )
        return files

    def get_prior_interface(self):
        if self._prior_interface is None:
            self._prior_interface = CwinpyPriorInterface(self.production.priors)
        return self._prior_interface

    # The configuration

    def before_config(self, dryrun=False):
        """
        Check that the ledger supplies everything that the configuration needs.

        Raises
        ------
        PipelineException
            If a value is missing, or the priors or the data cannot be used.
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
        self.render_priors()
        if self.fake():
            self.fake_asds()
        else:
            self.data_files()
        super().before_config(dryrun=dryrun)

    def render_priors(self):
        """The text of the prior file, as ``cwinpy_pe`` reads it."""
        try:
            return self.get_prior_interface().render()
        except ValueError as error:
            raise PipelineException(str(error), production=self.production.name) from error

    def fake_asds(self):
        """``{detector: asd}`` of the simulated noise, for each detector of the analysis."""
        asds = self.settings().get("fake asd") or {}
        missing = [d for d in self.interferometers() if d not in asds]
        if missing:
            raise PipelineException(
                f"`cwinpy-pe: fake asd` has no value for {', '.join(missing)}.",
                production=self.production.name,
            )
        return {detector: asds[detector] for detector in self.interferometers()}

    def output_directory(self):
        return os.path.join(self.production.rundir, "results")

    def prior_file(self):
        return os.path.join(self.production.rundir, f"{self.production.name}.prior")

    def label(self):
        return self.production.name

    def result_file(self):
        """The file that ``cwinpy_pe`` writes when it has finished."""
        return os.path.join(self.output_directory(), f"{self.label()}_result.hdf5")

    def render_config(self):
        """
        The text of the configuration of ``cwinpy_pe``.

        Built here and not in the template: which options appear depends on the data,
        and ``cwinpy_pe`` rejects options that it does not know.
        """
        sampler = self.production.meta.get("sampler") or {}
        settings = self.settings()
        lines = [
            f"par-file = {self.production.meta['ephemeris']}",
            f"detector = [{', '.join(self.interferometers())}]",
        ]
        if self.fake():
            asds = self.fake_asds()
            for factor in self.factors():
                values = ", ".join(f"{d}:{asds[d]}" for d in self.interferometers())
                lines.append(f"{FAKE_OPTIONS[factor]} = [{values}]")
            for key in ("fake start", "fake end", "fake dt", "fake seed"):
                if settings.get(key) is not None:
                    lines.append(f"{key.replace(' ', '-')} = {settings[key]}")
        else:
            for factor, by_detector in self.data_files().items():
                values = ", ".join(f"{d}:{path}" for d, path in by_detector.items())
                lines.append(f"{DATA_OPTIONS[factor]} = [{values}]")
        lines += [
            f"prior = {self.prior_file()}",
            f"outdir = {self.output_directory()}",
            f"label = {self.label()}",
        ]
        if sampler.get("sampler"):
            lines.append(f"sampler = {sampler['sampler']}")
        if sampler.get("sampler kwargs"):
            lines.append(f"sampler-kwargs = {sampler['sampler kwargs']}")
        restart = (self.production.meta.get("scheduler") or {}).get("periodic restart time")
        if restart is not None:
            lines.append(f"periodic-restart-time = {restart}")
        return "\n".join(lines)

    @property
    def config_template(self):
        """The bundled template that the configuration is made from."""
        return str(importlib.resources.files("asimov_cwinpy").joinpath("configs/cwinpy-pe.ini"))

    # Building and submitting

    def _executable(self):
        """
        The ``cwinpy_pe`` that will be run: the analysis's own ``executable``, else the one in
        the environment of the project's pipelines, else the one on the path.
        """
        try:
            default = os.path.join(config.get("pipelines", "environment"), "bin", "cwinpy_pe")
        except Exception:
            default = "cwinpy_pe"
        executable = self.production.meta.get("executable", default)
        found = shutil.which(executable) or shutil.which("cwinpy_pe")
        if found is None:
            raise PipelineException(
                "Cannot find the cwinpy_pe executable.", production=self.production.name
            )
        return found

    def build_dag(self, dryrun=False):
        """
        Write the prior file and find the configuration that asimov has made.

        Returns
        -------
        str
            The path of the configuration file.
        """
        rundir = self.production.rundir
        if rundir is None:
            raise PipelineException(
                "This analysis has no run directory: set `working directory` on the subject "
                "(or `rundir` on the analysis) in the ledger.",
                production=self.production.name,
            )
        if not dryrun:
            os.makedirs(rundir, exist_ok=True)
            with open(self.prior_file(), "w") as handle:
                handle.write(self.render_priors())

        repository = self.production.event.repository
        if repository:
            found = repository.find_prods(self.production.name, self.category)
            if not found:
                raise PipelineException(
                    f"No configuration file found for {self.production.name} in the event "
                    f"repository's '{self.category}' directory.",
                    production=self.production.name,
                )
            config_file = os.path.join(repository.directory, self.category, found[0])
        else:
            config_file = f"{self.production.name}.ini"
        if dryrun:
            print(f"Configuration file: {config_file}")
        return config_file

    def submit_dag(self, dryrun=False):
        """
        Submit the ``cwinpy_pe`` job to the scheduler.

        Returns
        -------
        int
            The identifier that the scheduler gave the job.
        """
        config_file = self.build_dag(dryrun=dryrun)
        rundir = self.production.rundir
        command = [self._executable(), "--config", config_file]
        asked = self.production.meta.get("scheduler", {}) or {}
        description = {
            "executable": command[0],
            "arguments": " ".join(command[1:]),
            "output": os.path.join(rundir, f"{self.production.name}.out"),
            "error": os.path.join(rundir, f"{self.production.name}.err"),
            "log": os.path.join(rundir, f"{self.production.name}.log"),
            "request_cpus": str(asked.get("request cpus", 1)),
            "request_memory": str(asked.get("request memory", "4096MB")),
            "request_disk": str(asked.get("request disk", "4096MB")),
            "getenv": "true",
            "batch_name": f"cwinpy-pe/{self.production.event.name}/{self.production.name}",
        }
        accounting = asked.get("accounting group")
        if accounting:
            description["accounting_group"] = accounting
            try:
                description["accounting_group_user"] = config.get("condor", "user")
            except Exception:
                pass
        self.logger.info(" ".join(command))
        if dryrun:
            print(f"Would submit: {' '.join(command)}")
            print(description)
            return None
        with set_directory(rundir):
            try:
                cluster = self.scheduler.submit(create_job_from_dict(description))
            except (FileNotFoundError, RuntimeError) as error:
                raise PipelineException(
                    f"The cwinpy_pe job could not be submitted: {error}",
                    production=self.production.name,
                ) from error
        self.production.status = "running"
        self.production.job_id = int(cluster)
        self.logger.info(f"Submitted job: {self.production.job_id}")
        return int(cluster)

    # Knowing how it went

    def detect_completion(self):
        """
        Whether ``cwinpy_pe`` has written its result.

        Not the checkpoint files, which it writes while it runs.
        """
        return bool(self.production.rundir) and os.path.exists(self.result_file())

    def _how_it_ended(self):
        """
        What the log of the job says about how it ended, if it has.

        HTCondor writes an event to the log for each time the job is submitted, ends or is
        removed, and the last one is what is true now. A job that has been submitted again
        since it last ended, as after a restart, has not ended.

        Returns
        -------
        tuple or None
            ``("exited", code)``, ``("killed", signal)`` or ``("removed",)``, or None if the
            job has not ended or there is no log (other schedulers do not write one).
        """
        path = os.path.join(self.production.rundir, f"{self.production.name}.log")
        try:
            with open(path) as handle:
                events = re.split(r"^\.\.\.\s*$", handle.read(), flags=re.MULTILINE)
        except OSError:
            return None
        ended = None
        for event in events:
            found = _EVENT.match(event)
            if not found:
                continue
            if found.group(1) == "000":
                ended = None
            elif found.group(1) == "009":
                ended = ("removed",)
            elif found.group(1) == "005":
                exited, killed = _EXITED.search(event), _KILLED.search(event)
                if exited:
                    ended = ("exited", int(exited.group(1)))
                elif killed:
                    ended = ("killed", int(killed.group(1)))
        return ended

    def _failed_with_an_exception(self):
        """Whether the error output of the job ends in the traceback of an exception."""
        path = os.path.join(self.production.rundir, f"{self.production.name}.err")
        try:
            with open(path, "rb") as handle:
                handle.seek(0, os.SEEK_END)
                handle.seek(max(0, handle.tell() - 20000))
                return _TRACEBACK in handle.read().decode("utf-8", errors="replace")
        except OSError:
            return False

    def resurrect(self):
        """
        The job is not on the scheduler and there is no result.

        That is not always a failure: the list of jobs that asimov has may be out of date, and
        a scheduler that asimov runs in another process cannot be asked. So this raises only
        when there is evidence of a failure, and otherwise leaves the job alone and is asked
        again in the next cycle.

        If ``cwinpy_pe`` stopped itself to be started again (exit code 77, as it does after
        ``periodic restart time``), the job is submitted again, and resumes from its checkpoint.

        Raises
        ------
        CwinpyError
            If the job ended without a result, or its error output shows an exception.
        """
        ended = self._how_it_ended()
        if ended == ("exited", RESTART_CODE):
            self.logger.info("cwinpy_pe stopped to be restarted; submitting it again")
            self.submit_dag()
            return
        if ended is not None:
            if ended[0] == "exited":
                how = f"exited with code {ended[1]}"
            elif ended[0] == "killed":
                how = f"was killed by signal {ended[1]}"
            else:
                how = "was removed from the scheduler"
            raise CwinpyError(
                f"cwinpy_pe for {self.production.name} {how} and left no result; "
                f"see {self.production.name}.err in its run directory.",
                production=self.production.name,
            )
        if self._failed_with_an_exception():
            raise CwinpyError(
                f"cwinpy_pe for {self.production.name} failed with an exception; "
                f"see {self.production.name}.err in its run directory.",
                production=self.production.name,
            )
        self.logger.debug("The job has gone, but nothing says that it failed; leaving it")

    def collect_assets(self):
        """
        What the analysis has made.

        Returns
        -------
        dict
            ``samples``, the result file, once it exists.
        """
        if not self.production.rundir:
            return {}
        return {"samples": self.result_file()} if self.detect_completion() else {}

    def samples(self, absolute=False):
        """The path of the result file."""
        return self.result_file()

    def output_paths(self):
        return {"samples": self.result_file()}
