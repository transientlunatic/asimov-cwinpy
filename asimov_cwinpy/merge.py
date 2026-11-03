"""
The merge pipeline: ``pipeline: cwinpy-merge``.

Concatenates, for each pulsar, detector and frequency factor, the heterodyned
files that the shards of a run made, with ``cwinpy_heterodyne_merge``. It
provides the merged files as an asset, ``{pulsar: {detector: {factor: path}}}``,
which the parameter estimation of each pulsar reads.
"""

from .common import CwinpyPipeline


class MergePipeline(CwinpyPipeline):
    """Merge the heterodyned files of the shards of a run."""

    name = "cwinpy-merge"
    pipeline_name = "cwinpy-merge"
    step = "6"
