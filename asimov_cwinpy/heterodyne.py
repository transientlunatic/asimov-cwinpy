"""
The heterodyne pipeline: ``pipeline: cwinpy-heterodyne``.

One analysis is one *shard* of an observing run: a span of time for some
detectors, heterodyned for every pulsar of the run in a single HTCondor DAG made
by cwinpy's ``HeterodyneDAGRunner``. The shards of a run belong to a "run"
subject, and the ``cwinpy-merge`` analysis of that subject needs all of them.
"""

from .common import CwinpyPipeline


class HeterodynePipeline(CwinpyPipeline):
    """Heterodyne one shard of an observing run, for all of its pulsars."""

    name = "cwinpy-heterodyne"
    pipeline_name = "cwinpy-heterodyne"
    step = "4"
