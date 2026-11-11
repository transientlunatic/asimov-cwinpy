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

from asimov import retention
from asimov.pipeline import PipelineException

from .common import CwinpyError, TransientCwinpyError
from .heterodyne import ASSET as SHARD_ASSET
from .heterodyne import HeterodynePipeline
from .singlejob import SingleJobPipeline

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


class MergePipeline(SingleJobPipeline):
    """Merge the heterodyned files of the shards of a run."""

    name = "cwinpy-merge"
    pipeline_name = "cwinpy-merge"
    job_module = "asimov_cwinpy.merge_job"
    plan_name = PLAN
    status_name = STATUS

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

    # Knowing how it went

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
