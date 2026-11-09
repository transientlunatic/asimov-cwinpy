"""The skeletons of the pipelines, and how a failure is classified for asimov's retry policy."""

from unittest.mock import MagicMock

import pytest

from asimov import retry
from asimov.pipeline import PipelineException
from asimov.strategies import StrategyError

from asimov_cwinpy.common import CwinpyError, TransientCwinpyError
from asimov_cwinpy.heterodyne import HeterodynePipeline
from asimov_cwinpy.merge import MergePipeline
from asimov_cwinpy.pe import PEPipeline
from asimov_cwinpy.strategy import RunStrategy

PIPELINES = [HeterodynePipeline, MergePipeline, PEPipeline]


def production(pipeline):
    mock = MagicMock()
    mock.pipeline = pipeline
    mock.name = "analysis"
    mock.event.name = "subject"
    return mock


@pytest.mark.parametrize("cls", PIPELINES)
def test_a_pipeline_is_made_for_an_analysis_which_names_it(cls):
    assert cls(production(cls.pipeline_name)).production.pipeline == cls.pipeline_name


@pytest.mark.parametrize("cls", PIPELINES)
def test_the_name_is_not_case_sensitive(cls):
    cls(production(cls.pipeline_name.upper()))


@pytest.mark.parametrize("cls", PIPELINES)
def test_a_pipeline_refuses_an_analysis_of_another(cls):
    other = next(c for c in PIPELINES if c is not cls)
    with pytest.raises(PipelineException, match="mismatch"):
        cls(production(other.pipeline_name))


def test_the_strategy_says_it_is_not_implemented():
    with pytest.raises(StrategyError, match="not implemented"):
        RunStrategy().expand({}, MagicMock())


def test_a_failure_is_deterministic_unless_it_says_otherwise():
    assert CwinpyError("the code failed").transient is False
    assert retry.classify(CwinpyError("the code failed")) == retry.DETERMINISTIC


def test_a_transient_failure_is_classified_so_by_asimovs_retry_policy():
    assert retry.classify(TransientCwinpyError("a frame file could not be read")) == retry.TRANSIENT


def test_the_failures_are_pipeline_exceptions():
    assert issubclass(CwinpyError, PipelineException)
    assert issubclass(TransientCwinpyError, CwinpyError)
