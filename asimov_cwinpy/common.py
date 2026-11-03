"""
What the three cwinpy pipelines share.

The pipelines are skeletons for now: they can be found by asimov and refuse a
blueprint that names the wrong pipeline, but building and submitting is not
implemented (see ``PLAN.md`` for the order in which it will be).
"""

try:
    import asimov.analysis_types  # noqa: F401
    import asimov.retry  # noqa: F401
except ImportError as error:  # pragma: no cover - depends on the installed asimov
    raise ImportError(
        "asimov-cwinpy needs a version of asimov with analysis types and the retry "
        "policy (etive-io/asimov#302 and #299), which is newer than 0.8.1. "
        "Install asimov from its main branch."
    ) from error

from asimov.pipeline import Pipeline, PipelineException  # noqa: E402

#: The name of the analysis type (``asimov.analysis_types``) which the cwinpy
#: pipelines' analyses are. Generic: a pulsar analysis has no gravitational-wave
#: transient metadata, and no PSDs are looked for.
ANALYSIS_TYPE = "cwinpy"


class CwinpyError(PipelineException):
    """
    A cwinpy analysis failed, in a way that trying again is unlikely to fix.

    Raised from :meth:`~asimov.pipeline.Pipeline.resurrect` when a job has left the
    scheduler without completing. Asimov's retry policy (``asimov.retry``) reads
    the ``transient`` attribute of the exception to decide how many times to try
    again; see :class:`TransientCwinpyError`.
    """

    transient = False


class TransientCwinpyError(CwinpyError):
    """
    A cwinpy analysis failed in a way that is plausibly not its own fault.

    The case that matters is a heterodyne job which could not read a frame file
    (a disk or machine that was temporarily down). Asimov retries a transient
    failure more times than a deterministic one, and waits as the analysis's
    ``retry`` block says between attempts.
    """

    transient = True


class CwinpyPipeline(Pipeline):
    """
    The base of the cwinpy pipelines.

    A subclass sets :attr:`pipeline_name` to the name it is registered under
    (the ``pipeline:`` of a blueprint), and :attr:`step` to the step of
    ``PLAN.md`` at which it is implemented.
    """

    #: The name the pipeline is registered under in ``asimov.pipelines``.
    pipeline_name = ""

    #: The step of ``PLAN.md`` which implements this pipeline.
    step = ""

    #: A cwinpy analysis is not a gravitational-wave transient one.
    analysis_type = ANALYSIS_TYPE

    #: Whether :meth:`get_actual_inputs` lists everything the pipeline reads
    #: from the analyses it needs.
    declares_inputs = False

    STATUS = {"wait", "stuck", "stopped", "running", "finished"}

    def __init__(self, production, category=None):
        super().__init__(production, category)
        # Catches an entry-point name that differs from what the analysis asked
        # for, before anything runs.
        if str(production.pipeline).lower() != self.pipeline_name:
            raise PipelineException(
                f"Pipeline mismatch: {self.pipeline_name} was asked to run an analysis "
                f"of pipeline {production.pipeline}."
            )

    def _not_implemented(self, what):
        raise NotImplementedError(
            f"{self.pipeline_name} cannot {what} yet: it is a skeleton. "
            f"It is implemented in step {self.step} of PLAN.md."
        )

    def build_dag(self, dryrun=False):
        """Build what is submitted (not implemented)."""
        self._not_implemented("be built")

    def submit_dag(self, dryrun=False):
        """Submit the analysis to the scheduler (not implemented)."""
        self._not_implemented("be submitted")
