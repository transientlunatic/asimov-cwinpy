"""
What the pipelines that run one job of their own, and have it say how it went, share.

``cwinpy-merge`` and ``cwinpy-summary`` are not cwinpy DAGs: each is one job, which asimov
describes to the scheduler, that reads a *plan* (a file that the build wrote) and writes a *status*
file (``{"status": "succeeded" | "failed", ...}``). The status file is how the pipeline knows what
happened, whichever scheduler ran the job; a job that dies without writing one has left no
evidence, and is not taken to have failed.
"""

import json
import os
import sys

from asimov import config
from asimov.pipeline import PipelineException
from asimov.scheduler_utils import create_job_from_dict
from asimov.utils import set_directory

from .common import CwinpyPipeline


class SingleJobPipeline(CwinpyPipeline):
    """
    A pipeline that is one job, ``python -m <job_module> <plan>``, which reads a plan and
    writes a status file. A subclass says which, and makes the plan in ``build_dag``.
    """

    #: The module that is run (``python -m``), which takes the path of the plan.
    job_module = ""

    #: What the build writes for the job to read, and what the job writes, in the run directory.
    plan_name = "plan.json"
    status_name = "status.json"

    def plan_file(self):
        return os.path.join(self.production.rundir, self.plan_name)

    def status_file(self):
        return os.path.join(self.production.rundir, self.status_name)

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
        command = [self._python(), "-m", self.job_module, plan]
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
            "batch_name": f"{self.pipeline_name}/{self.production.event.name}/{name}",
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
                    f"The {self.pipeline_name} job could not be submitted: {error}",
                    production=name,
                ) from error
        self.production.status = "running"
        self.production.job_id = int(cluster)
        self.logger.info(f"Submitted job: {self.production.job_id}")
        return int(cluster)

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
