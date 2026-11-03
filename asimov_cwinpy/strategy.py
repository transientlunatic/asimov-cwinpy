"""
The ``cwinpy-run`` strategy.

Expands a description of an observing run into the run subject, its heterodyne
shards and merge, and a subject with a parameter estimation for each pulsar.
Not implemented yet (step 7 of ``PLAN.md``).
"""

from asimov.strategies import Strategy, StrategyError


class RunStrategy(Strategy):
    """Expand an observing run into shards, a merge, and one analysis per pulsar."""

    name = "cwinpy-run"

    def expand(self, blueprint, context):
        raise StrategyError("The cwinpy-run strategy is not implemented yet (step 7 of PLAN.md).")
