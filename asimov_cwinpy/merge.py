"""
The merge pipeline: ``pipeline: cwinpy-merge``.

Joins, for each pulsar, detector and frequency factor, the heterodyned files that the
shards of a run made into one file, with cwinpy. It needs the ``cwinpy-heterodyne``
analyses of the run, and provides the merged files as an asset, ``heterodyned data``,
``{pulsar: {detector: {factor: path}}}``, which the parameter estimation of each pulsar
reads.

The merges are made by one job (see :mod:`asimov_cwinpy.merge_job`), from a plan that
the configuration of the analysis holds: which files of which shards make each merged
file. The configuration is that plan, written as JSON.
"""

import importlib.resources
import json
import os
import re
import sys

from asimov import config, retention
from asimov.pipeline import PipelineException
from asimov.scheduler_utils import create_job_from_dict
from asimov.utils import set_directory

from .common import CwinpyError, CwinpyPipeline, TransientCwinpyError
from .heterodyne import ASSET as SHARD_ASSET
from .heterodyne import HeterodynePipeline

#: The asset: ``{pulsar: {detector: {factor: path}}}``.
ASSET = "heterodyned data"

#: What the job writes to say how it went, in the run directory.
STATUS = "merge-status.json"

#: The plan that the job reads, in the run directory.
PLAN = "plan.json"

#: The start of the data in a file that the heterodyne wrote, from its name.
_START = re.compile(r"_(\d+)-(\d+)\.h(?:df)?5?$")


def number(factor):
    """A frequency factor as an integer if it is one, so that 2.0 and 2 are the same key."""
    factor = float(factor)
    return int(factor) if factor.is_integer() else factor


def by_time(path):
    """A key that puts the files of a pulsar in the order of the data in them."""
    found = _START.search(os.path.basename(path))
    return (int(found.group(1)) if found else 0, path)


class MergePipeline(CwinpyPipeline):
    """Merge the heterodyned files of the shards of a run."""

    name = "cwinpy-merge"
    pipeline_name = "cwinpy-merge"

    available_outputs = [ASSET, "config"]
    declares_inputs = True

    # What it merges

    def get_actual_inputs(self, production):
        return [SHARD_ASSET]

    def shards(self):
        """
        The files that each shard made.

        Returns
        -------
        list of (str, dict)
            The name of each shard it needs and its ``heterodyned files``, in name order.
            A shard that was given up on (``skipped``) is left out.

        Raises
        ------
        PipelineException
            If a shard is not complete, or there are none. Merging without one would
            leave a gap in the data and say nothing.
        """
        found = []
        for dependency in self.production.dependency_objects:
            if not isinstance(dependency.pipeline, HeterodynePipeline):
                continue
            if str(dependency.status).lower() == "skipped":
                self.logger.warning(f"{dependency.name} was given up on; merging without it")
                continue
            files = (dependency.pipeline.collect_assets() or {}).get(SHARD_ASSET)
            name = self.production._qualified_name(dependency)
            if not files and SHARD_ASSET in retention.released_names(dependency):
                raise PipelineException(
                    f"The heterodyned files of the shard {name} were released to save space "
                    f"({retention.released_when(dependency, SHARD_ASSET)}), after a merge had used "
                    f"them. To merge again, run the shard again.",
                    production=self.production.name,
                )
            if not files:
                raise PipelineException(
                    f"The shard {name} has no heterodyned files: it is not complete.",
                    production=self.production.name,
                )
            found.append((name, files))
        if not found:
            raise PipelineException(
                f"{self.production.name} needs no cwinpy-heterodyne analysis, so there is "
                f"nothing to merge.",
                production=self.production.name,
            )
        return found

    def output_directory(self, detector):
        return os.path.join(self.production.rundir, "results", detector)

    def items(self):
        """
        The merges to make.

        Returns
        -------
        list of dict
            ``{pulsar, detector, factor, inputs, output}`` for each pulsar, detector and
            factor that any shard has files for, with the files in time order.
        """
        groups = {}
        for _, files in self.shards():
            for pulsar, by_detector in files.items():
                for detector, by_factor in by_detector.items():
                    for factor, paths in by_factor.items():
                        group = groups.setdefault((pulsar, detector, number(factor)), [])
                        group.extend(paths)
        return [
            {
                "pulsar": pulsar,
                "detector": detector,
                "factor": factor,
                "inputs": sorted(set(paths), key=by_time),
                "output": os.path.join(
                    self.output_directory(detector), f"merged_{pulsar}_{detector}_{factor}f.hdf5"
                ),
            }
            for (pulsar, detector, factor), paths in sorted(groups.items())
        ]

    # The configuration, which is the plan

    def plan(self):
        """What the job reads: the merges to make, and where to say how it went."""
        return {
            "status": os.path.join(self.production.rundir, STATUS),
            "items": self.items(),
        }

    def render_config(self):
        """The text of the configuration: the plan, as JSON."""
        return json.dumps(self.plan(), indent=1)

    @property
    def config_template(self):
        """The bundled template that the configuration is made from."""
        return str(importlib.resources.files("asimov_cwinpy").joinpath("configs/cwinpy-merge.ini"))

    def before_config(self, dryrun=False):
        """
        Check that every shard it needs is complete.

        Raises
        ------
        PipelineException
            If a shard is not complete, or none is needed.
        """
        self.items()
        super().before_config(dryrun=dryrun)

    # Building and submitting

    def plan_file(self):
        return os.path.join(self.production.rundir, PLAN)

    def status_file(self):
        return os.path.join(self.production.rundir, STATUS)

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
        Put the plan where the job reads it, and make the directories it writes to.

        Returns
        -------
        str
            The path of the plan.
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
            return self.plan_file()
        try:
            with open(config_file) as handle:
                plan = json.load(handle)
        except ValueError as error:
            raise PipelineException(
                f"The configuration {config_file} is not the plan of a merge: {error}",
                production=self.production.name,
            ) from error
        os.makedirs(rundir, exist_ok=True)
        for item in plan["items"]:
            os.makedirs(os.path.dirname(item["output"]), exist_ok=True)
        with open(self.plan_file(), "w") as handle:
            json.dump(plan, handle, indent=1)
        return self.plan_file()

    def _python(self):
        """
        The Python that runs the job: the analysis's own ``executable``, else the one in the
        environment of the project's pipelines, else the one asimov is running in. It has to
        have this package, as well as cwinpy.
        """
        try:
            default = os.path.join(config.get("pipelines", "environment"), "bin", "python")
        except Exception:
            default = sys.executable
        executable = self.production.meta.get("executable", default)
        return executable if os.path.exists(executable) else sys.executable

    def submit_dag(self, dryrun=False):
        """
        Submit the job to the scheduler.

        Returns
        -------
        int
            The identifier that the scheduler gave the job.
        """
        plan = self.build_dag(dryrun=dryrun)
        rundir = self.production.rundir
        command = [self._python(), "-m", "asimov_cwinpy.merge_job", plan]
        asked = self.production.meta.get("scheduler", {}) or {}
        name = self.production.name
        description = {
            "executable": command[0],
            "arguments": " ".join(command[1:]),
            "output": os.path.join(rundir, f"{name}.out"),
            "error": os.path.join(rundir, f"{name}.err"),
            "log": os.path.join(rundir, f"{name}.log"),
            "request_cpus": str(asked.get("request cpus", 1)),
            "request_memory": str(asked.get("request memory", "4096MB")),
            "request_disk": str(asked.get("request disk", "4096MB")),
            "getenv": "true",
            "batch_name": f"cwinpy-merge/{self.production.event.name}/{name}",
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
            return None
        # What the last run said is not what this one will say.
        if os.path.exists(self.status_file()):
            os.remove(self.status_file())
        with set_directory(rundir):
            try:
                cluster = self.scheduler.submit(create_job_from_dict(description))
            except (FileNotFoundError, RuntimeError) as error:
                raise PipelineException(
                    f"The merge job could not be submitted: {error}", production=name
                ) from error
        self.production.status = "running"
        self.production.job_id = int(cluster)
        self.logger.info(f"Submitted job: {self.production.job_id}")
        return int(cluster)

    # Knowing how it went

    def job_status(self):
        """
        What the job said about how it went.

        Returns
        -------
        dict or None
            ``{"status": "succeeded" | "failed", ...}``, or None if it has said nothing.
        """
        try:
            with open(self.status_file()) as handle:
                return json.load(handle)
        except (OSError, ValueError):
            return None

    def planned(self):
        """The merges that the job was asked to make, or None if the plan is not there."""
        try:
            with open(self.plan_file()) as handle:
                return json.load(handle)["items"]
        except (OSError, ValueError, KeyError):
            return None

    def incomplete_outputs(self):
        """The merged files that are missing or empty."""
        found = []
        for item in self.planned() or []:
            try:
                if os.path.getsize(item["output"]) == 0:
                    found.append(item["output"])
            except OSError:
                found.append(item["output"])
        return found

    def detect_completion(self):
        """Whether the job said that it succeeded, and every merged file has data."""
        if not self.production.rundir or self.planned() is None:
            return False
        status = self.job_status()
        return (
            bool(status and status.get("status") == "succeeded") and not self.incomplete_outputs()
        )

    def resurrect(self):
        """
        The job has left the scheduler and the merge is not complete.

        That is not always a failure, so this raises only when the job said that it failed,
        or said that it succeeded and the merged files are not there. A job that has said
        nothing is left alone.

        Raises
        ------
        TransientCwinpyError
            If the job failed in a way that is likely not to happen again: a file system
            that was unwell.
        CwinpyError
            If it failed for another reason.
        """
        status = self.job_status()
        if status is None:
            self.logger.debug("The job has gone, but has said nothing; leaving it")
            return
        if status.get("status") == "failed":
            error = TransientCwinpyError if status.get("transient") else CwinpyError
            raise error(
                f"The merge job of {self.production.name} failed: {status.get('error')}. "
                f"See {self.production.name}.err in its run directory.",
                production=self.production.name,
            )
        incomplete = self.incomplete_outputs()
        if incomplete:
            raise CwinpyError(
                f"The merge job of {self.production.name} succeeded but {len(incomplete)} of its "
                f"merged files are missing or empty, for example {incomplete[0]}.",
                production=self.production.name,
            )

    def collect_assets(self):
        """
        What the merge has made.

        Returns
        -------
        dict
            ``heterodyned data``, ``{pulsar: {detector: {factor: path}}}``, once it is
            complete.
        """
        if not self.detect_completion():
            return {}
        data = {}
        for item in self.planned():
            data.setdefault(item["pulsar"], {}).setdefault(item["detector"], {})[item["factor"]] = (
                item["output"]
            )
        return {ASSET: data}

    def output_paths(self):
        return {ASSET: [item["output"] for item in self.planned() or []]}
