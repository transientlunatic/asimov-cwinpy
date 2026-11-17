"""What the jobs of the cwinpy pipelines say to HTCondor, apart from what they run."""

import types
from unittest.mock import patch

from asimov_cwinpy import jobs


def production(**meta):
    return types.SimpleNamespace(meta=meta)


class TestSoftwareEnvironment:
    def test_the_analysis_says_it(self):
        with patch.object(jobs.config, "get", return_value="/project/env"):
            assert jobs.software_environment(production(scheduler={"environment": "/x"})) == "/x"

    def test_otherwise_the_project_does(self):
        with patch.object(jobs.config, "get", return_value="/project/env"):
            assert jobs.software_environment(production()) == "/project/env"
            assert jobs.software_environment(production(scheduler={})) == "/project/env"
            assert jobs.software_environment(production(scheduler=None)) == "/project/env"

    def test_otherwise_there_is_none(self):
        with patch.object(jobs.config, "get", side_effect=KeyError("environment")):
            assert jobs.software_environment(production()) is None

    def test_an_empty_one_is_not_one(self):
        with patch.object(jobs.config, "get", return_value="/project/env"):
            assert jobs.software_environment(production(scheduler={"environment": ""})) == (
                "/project/env"
            )

    def test_it_is_a_string_whatever_the_yaml_made_of_it(self):
        assert jobs.software_environment(production(scheduler={"environment": 12})) == "12"

    def test_an_executable_in_it(self):
        env = production(scheduler={"environment": "/cvmfs/igwn"})
        assert jobs.executable_in(env, "cwinpy_pe") == "/cvmfs/igwn/bin/cwinpy_pe"

    def test_no_executable_without_one(self):
        with patch.object(jobs.config, "get", side_effect=KeyError("environment")):
            assert jobs.executable_in(production(), "python") is None


class TestFormatEnvironment:
    def test_pairs_are_separated_by_spaces_inside_double_quotes(self):
        assert jobs.format_environment({"A": "1", "B": "two"}) == '"A=1 B=two"'

    def test_the_order_is_the_order_given(self):
        assert jobs.format_environment({"Z": 1, "A": 2}) == '"Z=1 A=2"'

    def test_a_value_with_a_space_is_in_single_quotes(self):
        assert jobs.format_environment({"NOTE": "two words"}) == "\"NOTE='two words'\""

    def test_a_single_quote_is_doubled_and_the_value_quoted(self):
        assert jobs.format_environment({"N": "it's"}) == "\"N='it''s'\""

    def test_a_double_quote_is_doubled(self):
        assert jobs.format_environment({"N": 'say "hi"'}) == '"N=\'say ""hi""\'"'

    def test_an_empty_value_is_quoted_so_that_it_is_there(self):
        assert jobs.format_environment({"E": ""}) == "\"E=''\""

    def test_numbers_and_paths(self):
        assert jobs.format_environment({"N": 3, "P": "/cvmfs/x:/y"}) == '"N=3 P=/cvmfs/x:/y"'


class TestHtcondorOptions:
    def test_nothing_is_asked_unless_the_ledger_says_so(self):
        assert jobs.htcondor_options(production()) == {}
        assert jobs.htcondor_options(production(htcondor={}, scheduler={})) == {}
        assert jobs.htcondor_options(production(htcondor=None, scheduler=None)) == {}

    def test_in_particular_not_getenv(self):
        assert "getenv" not in jobs.htcondor_options(production(htcondor={"getenv": False}))

    def test_each_option_is_passed_on(self):
        options = jobs.htcondor_options(
            production(
                htcondor={"universe": "local", "requirements": "(X =?= True)", "getenv": True},
                scheduler={"environment variables": {"A": "b"}},
            )
        )
        assert options == {
            "universe": "local",
            "requirements": "(X =?= True)",
            "getenv": "true",
            "environment": '"A=b"',
        }

    def test_an_empty_mapping_of_variables_is_no_variables(self):
        assert jobs.htcondor_options(production(scheduler={"environment variables": {}})) == {}


class TestRunsLocally:
    def test_only_the_local_universe_is(self):
        assert jobs.runs_locally(production(htcondor={"universe": "local"}))
        assert not jobs.runs_locally(production(htcondor={"universe": "vanilla"}))
        assert not jobs.runs_locally(production())
        assert not jobs.runs_locally(production(htcondor=None))


def test_the_terms_for_the_vocabulary_are_there_with_their_types():
    assert jobs.HTCONDOR_TERMS["universe"]["type"] == "string"
    assert jobs.HTCONDOR_TERMS["requirements"]["type"] == "string"
    assert jobs.HTCONDOR_TERMS["getenv"]["type"] == "boolean"
    assert all(term["description"] for term in jobs.HTCONDOR_TERMS.values())


def test_the_analysis_type_brings_them_to_the_vocabulary():
    from asimov.vocabulary import get_vocabulary

    vocabulary = get_vocabulary(refresh=True)
    for name in jobs.HTCONDOR_TERMS:
        assert vocabulary.lookup(f"htcondor.{name}") is not None, name
    # and the standard terms that are used are asimov's own
    for name in ("transfer files", "environment", "environment variables"):
        assert vocabulary.lookup(f"scheduler.{name}") is not None, name


# Where the execute nodes do not see the project, nor the home directory


def on(**scheduler):
    return production(scheduler=scheduler)


def test_home_is_the_home_of_the_user_that_runs_asimov(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    assert jobs.in_home(str(tmp_path / ".conda" / "bin" / "python"))
    assert jobs.in_home(str(tmp_path))
    assert not jobs.in_home(str(tmp_path) + "-other/bin")
    assert not jobs.in_home("/cvmfs/software.igwn.org/conda/envs/igwn/bin/cwinpy_pe")


def test_a_home_executable_is_only_refused_where_the_nodes_do_not_see_the_project(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("HOME", str(tmp_path))
    path = str(tmp_path / ".conda" / "envs" / "x" / "bin" / "cwinpy_pe")
    assert jobs.not_for_the_nodes(on(), path) is None
    message = jobs.not_for_the_nodes(on(**{"transfer files": True}), path)
    assert "home directory" in message and "scheduler: environment" in message
    assert jobs.not_for_the_nodes(on(**{"transfer files": True}), "/cvmfs/igwn/bin/x") is None
