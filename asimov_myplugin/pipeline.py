"""
Example pipeline implementation for asimov.

This module demonstrates the pattern real asimov plugins converge on for a
tool that is a single executable taking one config file and running to
completion (this covers most modern PE codes -- pycbc_inference, jim-run,
bayeswave, ...). There is no DAG to build: `build_dag()` just checks the
run directory and finds the config path, and `submit_dag()` submits one job through
asimov's scheduler abstraction. If your tool genuinely has multiple
dependent stages, you'll need to build a real HTCondor DAG instead --
`self.scheduler` still applies, but you'd call `self.scheduler.submit_dag()`
with a `.dag` file rather than `self.scheduler.submit()` with one job.
"""

import glob
import importlib.resources
import logging
import os
import shutil

from asimov import config
from asimov.pipeline import Pipeline, PipelineException
from asimov.scheduler import Slurm
from asimov.scheduler_utils import create_job_from_dict
from asimov.utils import set_directory

logger = logging.getLogger(__name__)


class MyPlugin(Pipeline):
    """
    Example single-job pipeline implementation.

    Rename this class (and the module/package it lives in) to match your
    own analysis code, and replace ``my-executable``/``myplugin`` below
    with its real name throughout.
    """

    name = "My Plugin"
    STATUS = {"wait", "stuck", "stopped", "running", "finished"}

    def __init__(self, production, category=None):
        super().__init__(production, category)
        # Guard against this class being registered under an entry-point
        # name that doesn't match what's actually configured for this
        # production -- catches a ledger typo early, before anything runs.
        if not production.pipeline.lower() == "myplugin":
            raise PipelineException("Pipeline mismatch")

    #: Ledger keys the config template needs and which have no safe default.
    #: A missing sample rate, frequency or approximant is a scientific choice
    #: the ledger must make, so ``before_config`` refuses to render without it.
    REQUIRED_META = (
        ("event time",),
        ("interferometers",),
        ("likelihood", "sample rate"),
        ("likelihood", "minimum frequency"),
        ("waveform", "approximant"),
    )

    def before_config(self, dryrun=False):
        """
        Check the ledger supplies everything the config template needs.

        Called immediately before ``production.make_config()`` (see
        ``asimov/cli/manage.py``). This hook must not write anything into
        ``self.production.meta``: the ledger holds analysis *inputs*, not
        values derived from them. Anything the template needs to compute
        (converted priors, a collapsed per-detector value, ...) is a method
        on this class which the template calls directly, e.g.
        ``{{ pipeline.render_priors() }}``, because ``Analysis.make_config``
        renders the template with ``pipeline`` in scope.

        Parameters
        ----------
        dryrun : bool, optional
            Unused; accepted for compatibility with the CLI.

        Raises
        ------
        PipelineException
            If a required ledger value is missing.
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

    def minimum_frequency(self):
        """
        Return the single low-frequency cutoff passed to ``my-executable``.

        ``likelihood: minimum frequency`` is given per detector. This tool
        takes one value, so the *lowest* of the per-detector values is used
        (as the vocabulary recommends), which never discards data a
        detector was configured to analyse.

        Returns
        -------
        float
            The lowest per-detector minimum frequency, in Hz.

        Raises
        ------
        PipelineException
            If a detector in ``interferometers`` has no minimum frequency.
        """
        frequencies = self.production.meta["likelihood"]["minimum frequency"]
        ifos = self.production.meta["interferometers"]
        missing = [ifo for ifo in ifos if ifo not in frequencies]
        if missing:
            raise PipelineException(
                f"`likelihood: minimum frequency` has no value for {', '.join(missing)}.",
                production=self.production.name,
            )
        return min(float(frequencies[ifo]) for ifo in ifos)

    def render_priors(self):
        """
        Render the ``[prior]`` section body for the config template.

        The template calls this as ``{{ pipeline.render_priors() }}``, so the
        converted priors never have to be stored in the ledger. Priors come
        only from the standard ``priors`` block, converted by
        :class:`~asimov_myplugin.priors.MyPluginPriorInterface`; there is no
        plugin-specific prior block to fall back on.

        Returns
        -------
        str
            One ``name = type(args)`` line per prior.

        Raises
        ------
        PipelineException
            If a prior uses a type the interface cannot convert.
        """
        try:
            converted = self.get_prior_interface().convert()
        except ValueError as error:
            raise PipelineException(str(error), production=self.production.name) from error
        lines = []
        for name, spec in converted.items():
            if spec["type"] == "gaussian":
                lines.append(f"{name} = gaussian({spec['mean']}, {spec['std']})")
            else:
                lines.append(f"{name} = {spec['type']}({spec['min']}, {spec['max']})")
        return "\n".join(lines)

    @property
    def config_template(self):
        """
        Bundled Liquid template used to render this production's config
        file when one doesn't already exist in the event repository.

        ``Production.make_config()`` only falls back to this when no
        global ``[templating] directory`` is set in ``asimov.conf``; if one
        is configured, Asimov looks there for a file literally named
        ``<pipeline-name>.ini`` regardless of this property or your
        template's real extension (see
        https://github.com/etive-io/asimov/issues/160 -- filed upstream,
        tracked there rather than worked around here).
        """
        return str(importlib.resources.files("asimov_myplugin").joinpath("configs/myplugin.ini"))

    def _executable(self):
        """
        Resolve the analysis executable defensively rather than assuming
        ``config["pipelines"]["environment"]/bin/my-executable`` exists --
        in minimal/containerised environments that config value may not
        point at the active environment. ``shutil.which`` also picks up an
        explicit per-production override via ``production.meta["executable"]``.
        """
        default_executable = os.path.join(
            config.get("pipelines", "environment"), "bin", "my-executable"
        )
        executable = self.production.meta.get("executable", default_executable)
        executable = shutil.which(executable) or shutil.which("my-executable")
        if executable is None:
            raise PipelineException(
                "Cannot find the my-executable executable",
                production=self.production.name,
            )
        return executable

    def build_dag(self, dryrun=False):
        """
        Check the run directory and locate this production's rendered
        config file in the event repository.

        The run directory comes from ``self.production.rundir``, which
        ``Analysis.rundir`` derives from the subject's ``working directory``;
        the plugin never builds that path itself.

        There's no separate DAG-building step for a single-job tool -- it
        is submitted directly as one job (see ``submit_dag``) -- but
        asimov's generic ``manage build submit`` CLI unconditionally calls
        ``build_dag`` on every pipeline before ``submit_dag``, so this must
        exist. Deliberately does *not* call ``make_config()`` itself: that
        is the separate, earlier ``asimov manage build`` CLI step, and
        conflating the two breaks the documented build -> submit
        separation (see the ``asimov build``/``asimov submit`` sections of
        Asimov's own docs).
        """
        rundir = self.production.rundir
        if rundir is None:
            raise PipelineException(
                "This analysis has no run directory: set `working directory` on "
                "the subject (or `rundir` on the analysis) in the ledger.",
                production=self.production.name,
            )
        if not dryrun:
            os.makedirs(rundir, exist_ok=True)

        if self.production.event.repository:
            configs = self.production.event.repository.find_prods(
                self.production.name, self.category
            )
            if not configs:
                raise PipelineException(
                    f"No configuration file found for {self.production.name} "
                    f"in the event repository's '{self.category}' directory.",
                    production=self.production.name,
                )
            config_file = os.path.join(
                self.production.event.repository.directory, self.category, configs[0]
            )
        else:
            config_file = f"{self.production.name}.ini"

        if dryrun:
            print(f"Configuration file: {config_file}")

        return config_file

    def submit_dag(self, dryrun=False):
        """
        Submit one job to the scheduler.

        Uses Asimov's scheduler abstraction (``self.scheduler``, HTCondor
        or Slurm, via ``asimov.scheduler_utils``) rather than hand-rolling
        ``htcondor``/``htcondor2`` calls or a ``condor_submit_dag``
        subprocess directly -- this is what makes the plugin work
        unmodified on either scheduler.
        """
        config_file = self.build_dag(dryrun=dryrun)
        rundir = self.production.rundir

        command = [self._executable(), config_file]
        scheduler_meta = self.production.meta.get("scheduler", {})
        batch_name = f"MyPlugin/{self.production.event.name}/{self.production.name}"

        self.logger.info(" ".join(command))

        submit_description = {
            "executable": command[0],
            "arguments": " ".join(command[1:]),
            "output": os.path.join(rundir, f"{self.production.name}.out"),
            "error": os.path.join(rundir, f"{self.production.name}.err"),
            "log": os.path.join(rundir, f"{self.production.name}.log"),
            "request_cpus": str(scheduler_meta.get("request cpus", 1)),
            "request_memory": str(scheduler_meta.get("request memory", "4096MB")),
            "request_disk": str(scheduler_meta.get("request disk", "4096MB")),
            "getenv": "true",
            "batch_name": batch_name,
        }

        # GPU requests aren't part of the generic cpus/memory/disk resource
        # mapping (feature request filed upstream:
        # https://github.com/etive-io/asimov/issues/161), so they need
        # scheduler-specific keys, branching on scheduler type. Only
        # relevant if your analysis code needs a GPU (common for
        # JAX/PyTorch-based samplers) -- drop this block entirely otherwise.
        gpus = scheduler_meta.get("request gpus", 0)
        if gpus:
            if isinstance(self.scheduler, Slurm):
                submit_description["slurm_gres"] = f"gpu:{gpus}"
            else:
                submit_description["request_gpus"] = str(gpus)

        accounting_group = scheduler_meta.get("accounting group")
        if accounting_group:
            submit_description["accounting_group_user"] = config.get("condor", "user")
            submit_description["accounting_group"] = accounting_group

        if dryrun:
            print(f"Would submit: {' '.join(command)}")
            print("SUBMIT DESCRIPTION")
            print("-------------------")
            print(submit_description)
            return None

        with set_directory(rundir):
            try:
                job = create_job_from_dict(submit_description)
                cluster_id = self.scheduler.submit(job)
            except (FileNotFoundError, RuntimeError) as error:
                raise PipelineException(
                    f"The my-executable job could not be submitted: {error}",
                    production=self.production.name,
                ) from error

        self.production.status = "running"
        self.production.job_id = int(cluster_id)
        self.logger.info(f"Submitted job: {self.production.job_id}")
        return int(cluster_id)

    def detect_completion(self):
        """
        Check for a completion marker file.

        Prefer the *last* file your tool writes on a successful run (check
        its actual write order in source), not the first -- e.g. samples
        are commonly written before final diagnostics/config-dump files,
        so the latter is the more reliable completion signal.
        """
        completion_file = os.path.join(self.production.rundir, "output", "result.dat")
        return os.path.exists(completion_file)

    def samples(self):
        """Collect the samples file for downstream post-processing."""
        samples_file = os.path.join(self.production.rundir, "output", "samples.dat")
        return [samples_file] if os.path.exists(samples_file) else []

    def collect_assets(self):
        """
        Gather this production's result assets so a downstream production
        (e.g. a post-processing step wired up via ``needs:``, see §6 of
        the ``asimov-plugin`` skill) can pick them up through
        ``production._previous_assets()``.
        """
        assets = {"samples": self.samples()}
        if self.production.event.repository:
            configs = self.production.event.repository.find_prods(
                self.production.name, self.category
            )
            if configs:
                assets["config"] = configs[0]
        return assets

    def collect_logs(self):
        """Collect this production's log files, keyed by filename."""
        logs = (
            glob.glob(f"{self.production.rundir}/*.err")
            + glob.glob(f"{self.production.rundir}/*.out")
            + glob.glob(f"{self.production.rundir}/*.log")
        )
        messages = {}
        for log in logs:
            with open(log, "r") as log_f:
                messages[os.path.basename(log)] = log_f.read()
        return messages

    def after_completion(self):
        """
        Mark the production finished. Don't chain into post-processing
        (e.g. PESummary) from here -- express that as a separate
        production with a ``needs:`` dependency instead, so this plugin
        doesn't need to know what (if anything) consumes its output.
        """
        super().after_completion()

    #: How many times ``resurrect`` resubmits before giving up.
    MAX_RESURRECTIONS = 5

    def resurrect(self):
        """
        Resubmit on eviction/failure, capped at ``MAX_RESURRECTIONS``.

        If your tool auto-resumes from its own checkpoint file whenever
        one is present (with no special flag), resubmitting the exact
        same command is enough. If it instead needs an explicit flag
        suppressed once a checkpoint exists (e.g. a ``--force`` flag that
        must be dropped on resume), branch on that in ``submit_dag``
        rather than here.

        The retry count is kept in a file in the run directory, not in
        ``production.meta``: it is pipeline state, not an analysis input,
        and asimov core has no retry-count field for it yet (a gap worth
        raising upstream rather than copying into every plugin).
        """
        counter = os.path.join(self.production.rundir, ".resurrections")
        try:
            with open(counter) as handle:
                count = int(handle.read().strip() or 0)
        except FileNotFoundError:
            count = 0
        if count < self.MAX_RESURRECTIONS:
            with open(counter, "w") as handle:
                handle.write(str(count + 1))
            self.submit_dag()

    def get_prior_interface(self):
        from .priors import MyPluginPriorInterface

        if self._prior_interface is None:
            self._prior_interface = MyPluginPriorInterface(self.production.priors)
        return self._prior_interface
