"""
The parameter estimation pipeline: ``pipeline: cwinpy-pe``.

One analysis is one pulsar: a single ``cwinpy_pe`` job, reading that pulsar's
slice of the asset of the merge analysis it needs.
"""

from .common import CwinpyPipeline


class PEPipeline(CwinpyPipeline):
    """Estimate the parameters of the signal of one pulsar."""

    name = "cwinpy-pe"
    pipeline_name = "cwinpy-pe"
    step = "3"
