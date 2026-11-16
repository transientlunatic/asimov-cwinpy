"""
An ephemeris from the real ATNF catalogue is one that the real cwinpy heterodynes.

Needs cwinpy and the network (the catalogue is reached with psrqpy), and is skipped without cwinpy,
or if the catalogue cannot be reached: it is not a service that the tests can depend on.
"""

import os
import sys
from pathlib import Path

import pytest

from asimov.strategies import StrategyError

from . import minidag
from .test_heterodyne_cwinpy import blueprint, frames, shard  # noqa: F401

BIN = Path(sys.executable).parent

pytestmark = pytest.mark.skipif(
    not (BIN / "cwinpy_heterodyne").exists(), reason="cwinpy is not installed"
)


def test_the_ephemeris_of_the_crab_from_the_catalogue_is_heterodyned_by_cwinpy(
    project, monkeypatch, frames, tmp_path  # noqa: F811
):
    from asimov_cwinpy.ephemerides import ensure_atnf, pulsar_name

    try:
        path = ensure_atnf("J0534+2200", str(tmp_path / "ephemerides"))
    except StrategyError as error:
        if "could not be got" in str(error):
            pytest.skip(f"the ATNF catalogue cannot be reached: {error}")
        raise
    assert pulsar_name(path) == "J0534+2200"
    assert os.path.exists(path[: -len(".par")] + ".atnf.json")

    from cwinpy.parfile import PulsarParameters
    from cwinpy.utils import is_par_file

    assert is_par_file(path)
    assert PulsarParameters(path)["F0"] > 0

    # the jobs of the DAG that cwinpy makes read it, and make data
    text = blueprint(frames, frames["caches"]).replace(str(frames["root"] / "J0534+2200.par"), path)
    pipeline = shard(project, monkeypatch, text)
    codes = minidag.run_dag(pipeline.dag_file(), str(tmp_path / "scratch"))
    assert set(codes.values()) == {0}, codes
    assert pipeline.incomplete_outputs() == []
