"""The plugin is found by asimov through its entry points."""

import pytest

from asimov.analysis import GravitationalWaveTransient, SimpleAnalysis
from asimov.analysis_types import analysis_types, reset_analysis_types
from asimov.pipelines import known_pipelines
from asimov.strategies import Strategy, strategy_plugins
from asimov.vocabulary import get_vocabulary

from asimov_cwinpy.common import ANALYSIS_TYPE
from asimov_cwinpy.heterodyne import HeterodynePipeline
from asimov_cwinpy.merge import MergePipeline
from asimov_cwinpy.pe import PEPipeline
from asimov_cwinpy.types import CwinpyAnalysis

PIPELINES = {
    "cwinpy-heterodyne": HeterodynePipeline,
    "cwinpy-merge": MergePipeline,
    "cwinpy-pe": PEPipeline,
}


@pytest.fixture(autouse=True)
def fresh_registry():
    reset_analysis_types()
    yield
    reset_analysis_types()


@pytest.mark.parametrize("name, cls", sorted(PIPELINES.items()))
def test_the_pipelines_are_registered_under_their_ledger_names(name, cls):
    assert known_pipelines[name] is cls


def test_the_pipelines_say_what_they_are_called():
    for name, cls in PIPELINES.items():
        assert cls.pipeline_name == name
        assert cls.name == name


def test_the_analysis_type_is_registered_and_is_a_generic_one():
    found = analysis_types()[ANALYSIS_TYPE]
    assert found is CwinpyAnalysis
    assert issubclass(found, SimpleAnalysis)
    assert not issubclass(found, GravitationalWaveTransient)


@pytest.mark.parametrize("cls", sorted(PIPELINES.values(), key=lambda c: c.name))
def test_every_pipeline_declares_the_type_so_that_its_analyses_are_not_gravitational_wave(cls):
    assert cls.analysis_type == ANALYSIS_TYPE


def test_the_strategy_is_registered():
    found = strategy_plugins()["cwinpy-run"]
    assert issubclass(found, Strategy)
    assert found.name == "cwinpy-run"


def test_the_shared_terms_are_in_the_vocabulary_and_belong_to_no_pipeline():
    vocabulary = get_vocabulary(refresh=True)
    for path in ("ephemeris", "frequency factors"):
        term = vocabulary.lookup(path)
        assert term is not None, path
        assert term.owner is None, path


def test_the_heterodyne_terms_belong_to_the_heterodyne_pipeline():
    vocabulary = get_vocabulary(refresh=True)
    for path in ("data start", "data end", "joblength"):
        term = vocabulary.lookup(f"cwinpy-heterodyne.{path}")
        assert term is not None, path
        assert term.owner == "cwinpy-heterodyne", path


def test_the_vocabulary_does_not_clash_with_the_standard_one():
    assert get_vocabulary(refresh=True).conflicts == []


def test_the_pe_terms_belong_to_the_pe_pipeline():
    vocabulary = get_vocabulary(refresh=True)
    for path in ("fake asd", "fake start", "fake end", "fake dt", "fake seed"):
        term = vocabulary.lookup(f"cwinpy-pe.{path}")
        assert term is not None, path
        assert term.owner == "cwinpy-pe", path


def test_every_registered_vocabulary_loads():
    """A vocabulary that fails to load is skipped with a warning, and its namespace then
    goes unchecked, so `asimov vocabulary check` passes without checking anything."""
    from importlib.metadata import entry_points

    registered = {e.name for e in entry_points(group="asimov.vocabulary") if "cwinpy" in e.name}
    assert registered == {"cwinpy-heterodyne", "cwinpy-pe"}
    assert registered <= set(get_vocabulary(refresh=True).plugins)


def test_the_standard_terms_the_heterodyne_pipeline_reads_are_not_registered_again():
    vocabulary = get_vocabulary(refresh=True)
    for path in ("data.channels", "data.frame types", "data.segments", "data.cache files"):
        term = vocabulary.lookup(path)
        assert term is not None and term.owner is None, path
    for path in ("segment lists", "segment server", "strict data requirement", "ephemerides"):
        assert vocabulary.lookup(f"cwinpy-heterodyne.{path}") is not None, path
