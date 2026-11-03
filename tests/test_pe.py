"""The cwinpy-pe pipeline."""

import os
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from asimov.pipeline import PipelineException

from asimov_cwinpy.common import CwinpyError
from asimov_cwinpy.pe import DATA_ASSET, PEPipeline

from .conftest import SIMULATED, pulsar_blueprint

#: What the merge of a run provides.
MERGED = {
    DATA_ASSET: {
        "J0534+2200": {
            "H1": {1.0: "/m/H1_1f.hdf5", 2.0: "/m/H1_2f.hdf5"},
            "L1": {"1.0": "/m/L1_1f.hdf5", "2.0": "/m/L1_2f.hdf5"},
        }
    }
}


def make(project, simulated=False, extra_event="", extra_analysis="", provided=MERGED):
    """The pipeline of the analysis of a pulsar."""
    analysis = (SIMULATED if simulated else "") + extra_analysis
    project.apply(pulsar_blueprint(extra_event, analysis))
    production = project.analysis("J0534+2200", "pe")
    production._previous_assets = lambda: provided
    return production.pipeline


def lines(pipeline):
    return pipeline.render_config().splitlines()


# What it reads


def test_it_needs_the_heterodyned_data(project):
    assert make(project).get_actual_inputs(None) == [DATA_ASSET]


def test_it_needs_nothing_when_the_data_is_simulated(project):
    assert make(project, simulated=True).get_actual_inputs(None) == []


def test_the_data_of_the_pulsar_is_found_by_detector_and_factor(project):
    pipeline = make(project, extra_event="frequency factors: [1, 2]\n")
    assert pipeline.data_files() == {
        1: {"H1": "/m/H1_1f.hdf5", "L1": "/m/L1_1f.hdf5"},
        2: {"H1": "/m/H1_2f.hdf5", "L1": "/m/L1_2f.hdf5"},
    }


def test_it_analyses_twice_the_rotation_frequency_unless_told_otherwise(project):
    assert make(project).factors() == [2]


def test_a_pulsar_the_merge_does_not_have_is_an_error(project):
    pipeline = make(project, provided={DATA_ASSET: {"J0000+0000": {}}})
    with pytest.raises(PipelineException, match="provides heterodyned data for J0534\\+2200"):
        pipeline.data_files()


def test_nothing_provided_at_all_is_an_error(project):
    with pytest.raises(PipelineException, match="provides heterodyned data"):
        make(project, provided={}).data_files()


def test_a_detector_the_merge_lacks_is_an_error(project):
    data = {DATA_ASSET: {"J0534+2200": {"H1": {2.0: "/m/H1_2f.hdf5"}}}}
    with pytest.raises(PipelineException, match="lacks L1 at 2f"):
        make(project, provided=data).data_files()


def test_a_factor_cwinpy_pe_has_no_option_for_is_an_error(project):
    pipeline = make(project, extra_event="frequency factors: [3]\n")
    with pytest.raises(PipelineException, match="not 3"):
        pipeline.factors()


# The configuration


def test_the_configuration_for_heterodyned_data(project):
    pipeline = make(project)
    config = lines(pipeline)
    assert config[0] == "par-file = /data/J0534+2200.par"
    assert "detector = [H1, L1]" in config
    assert "data-file-2f = [H1:/m/H1_2f.hdf5, L1:/m/L1_2f.hdf5]" in config
    assert f"prior = {pipeline.prior_file()}" in config
    assert f"outdir = {pipeline.output_directory()}" in config
    assert "label = pe" in config
    assert not any(line.startswith(("fake", "data-file-1f")) for line in config)


def test_both_factors_have_their_own_option(project):
    config = lines(make(project, extra_event="frequency factors: [1, 2]\n"))
    assert "data-file-1f = [H1:/m/H1_1f.hdf5, L1:/m/L1_1f.hdf5]" in config
    assert "data-file-2f = [H1:/m/H1_2f.hdf5, L1:/m/L1_2f.hdf5]" in config


def test_the_configuration_for_simulated_noise(project):
    config = lines(make(project, simulated=True, provided={}))
    assert "fake-asd-2f = [H1:1e-24, L1:1e-24]" in config
    for option in ("fake-start = 1000000000", "fake-end = 1000086400", "fake-dt = 60"):
        assert option in config
    assert "fake-seed = 1" in config
    assert not any(line.startswith("data-file") for line in config)


def test_the_sampler_is_passed_on(project):
    config = lines(
        make(
            project,
            extra_analysis="sampler:\n  sampler: dynesty\n  sampler kwargs: {nlive: 100}\n",
        )
    )
    assert "sampler = dynesty" in config
    assert "sampler-kwargs = {'nlive': 100}" in config


def test_no_sampler_is_named_unless_the_analysis_names_one(project):
    assert not any(line.startswith("sampler") for line in lines(make(project)))


def test_the_restart_time_is_the_standard_scheduler_term(project):
    config = lines(make(project, extra_analysis="scheduler:\n  periodic restart time: 3600\n"))
    assert "periodic-restart-time = 3600" in config


def test_the_text_has_no_blank_lines_which_cwinpy_pe_would_trip_on(project):
    assert all(lines(make(project)))


def test_asimov_makes_the_configuration_file_from_the_template(project, tmp_path):
    pipeline = make(project)
    target = tmp_path / "pe.ini"
    pipeline.production.make_config(str(target))
    text = target.read_text()
    assert pipeline.render_config() in text
    assert "created by asimov for J0534+2200/pe" in text


# Checking the ledger


@pytest.fixture(autouse=True)
def no_environment_capture():
    """Capturing the software environment runs package managers, which these tests do not need."""
    with patch.object(PEPipeline, "_capture_environment"):
        yield


def test_a_complete_analysis_passes_the_check(project):
    make(project).before_config()


def test_the_check_names_what_is_missing(project):
    project.apply(
        "kind: event\nname: J0534+2200\n---\nkind: analysis\nname: pe\n"
        "pipeline: cwinpy-pe\nevent: J0534+2200\n"
    )
    pipeline = project.analysis("J0534+2200", "pe").pipeline
    with pytest.raises(PipelineException, match="ephemeris, interferometers, priors"):
        pipeline.before_config()


def test_a_prior_that_cannot_be_converted_fails_the_check(project):
    pipeline = make(project, extra_event="")
    pipeline.production.meta["priors"] = {"x": {"type": "cauchy", "minimum": 0.0, "maximum": 1.0}}
    pipeline._prior_interface = None
    with pytest.raises(PipelineException, match="cwinpy-pe can use"):
        pipeline.before_config()


def test_missing_data_fails_the_check(project):
    with pytest.raises(PipelineException, match="provides heterodyned data"):
        make(project, provided={}).before_config()


def test_simulated_noise_needs_a_value_for_every_detector(project):
    pipeline = make(project, extra_analysis="cwinpy-pe:\n  fake asd: {H1: 1.0e-24}\n", provided={})
    with pytest.raises(PipelineException, match="no value for L1"):
        pipeline.before_config()


def test_the_check_captures_the_environment_and_writes_nothing_else_to_the_ledger(project):
    pipeline = make(project)
    before = dict(pipeline.production.meta)
    pipeline.before_config()
    PEPipeline._capture_environment.assert_called_once()
    assert pipeline.production.meta == before


# Building and submitting


def test_building_writes_the_prior_file_and_finds_the_configuration(project, tmp_path):
    pipeline = make(project)
    project.analysis("J0534+2200", "pe")
    pipeline.production.event.repository = MagicMock(directory="/repo")
    pipeline.production.event.repository.find_prods.return_value = ["pe.ini"]
    found = pipeline.build_dag()
    assert found == os.path.join("/repo", pipeline.category, "pe.ini")
    prior = open(pipeline.prior_file()).read()
    assert "h0 = Uniform(minimum=0.0, maximum=1e-22, name='h0')" in prior
    assert "iota = Sine(" in prior


def test_building_without_a_configuration_is_an_error(project):
    pipeline = make(project)
    pipeline.production.event.repository = MagicMock(directory="/repo")
    pipeline.production.event.repository.find_prods.return_value = []
    with pytest.raises(PipelineException, match="No configuration file found"):
        pipeline.build_dag()


def test_a_dry_run_builds_nothing(project):
    pipeline = make(project)
    pipeline.production.event.repository = MagicMock(directory="/repo")
    pipeline.production.event.repository.find_prods.return_value = ["pe.ini"]
    pipeline.build_dag(dryrun=True)
    assert not os.path.exists(pipeline.prior_file())


@pytest.fixture
def submitting(project):
    """A pipeline that can be submitted, with a scheduler to watch."""
    pipeline = make(project)
    pipeline.production.event.repository = MagicMock(directory="/repo")
    pipeline.production.event.repository.find_prods.return_value = ["pe.ini"]
    scheduler = MagicMock()
    scheduler.submit.return_value = 42
    with patch.object(PEPipeline, "scheduler", new_callable=PropertyMock, return_value=scheduler):
        with patch("asimov_cwinpy.pe.shutil.which", return_value="/env/bin/cwinpy_pe"):
            yield pipeline, scheduler


def test_submitting_runs_cwinpy_pe_with_the_configuration(submitting):
    pipeline, scheduler = submitting
    assert pipeline.submit_dag() == 42
    job = scheduler.submit.call_args[0][0]
    assert job.executable == "/env/bin/cwinpy_pe"
    assert (
        job.kwargs["arguments"] == f"--config {os.path.join('/repo', pipeline.category, 'pe.ini')}"
    )
    assert pipeline.production.status == "running"
    assert pipeline.production.job_id == 42


def test_the_job_asks_for_the_resources_in_the_ledger(project):
    pipeline = make(
        project,
        extra_analysis="scheduler:\n  request cpus: 4\n  request memory: 8192MB\n",
    )
    pipeline.production.event.repository = MagicMock(directory="/repo")
    pipeline.production.event.repository.find_prods.return_value = ["pe.ini"]
    scheduler = MagicMock()
    scheduler.submit.return_value = 1
    with patch.object(PEPipeline, "scheduler", new_callable=PropertyMock, return_value=scheduler):
        with patch("asimov_cwinpy.pe.shutil.which", return_value="/env/bin/cwinpy_pe"):
            pipeline.submit_dag()
    job = scheduler.submit.call_args[0][0]
    assert str(job.kwargs["cpus"]) == "4"
    assert str(job.kwargs["memory"]) == "8192MB"


def test_a_dry_run_submits_nothing(submitting):
    pipeline, scheduler = submitting
    assert pipeline.submit_dag(dryrun=True) is None
    scheduler.submit.assert_not_called()


def test_a_scheduler_that_fails_is_a_pipeline_error(submitting):
    pipeline, scheduler = submitting
    scheduler.submit.side_effect = RuntimeError("no schedd")
    with pytest.raises(PipelineException, match="could not be submitted: no schedd"):
        pipeline.submit_dag()


def test_a_missing_executable_is_a_pipeline_error(project):
    pipeline = make(project)
    pipeline.production.event.repository = MagicMock(directory="/repo")
    pipeline.production.event.repository.find_prods.return_value = ["pe.ini"]
    with patch("asimov_cwinpy.pe.shutil.which", return_value=None):
        with pytest.raises(PipelineException, match="Cannot find the cwinpy_pe"):
            pipeline.submit_dag()


# Knowing how it went


def results(pipeline, *names):
    os.makedirs(pipeline.output_directory(), exist_ok=True)
    for name in names:
        open(os.path.join(pipeline.output_directory(), name), "w").close()


def test_it_is_not_complete_before_the_result_exists(project):
    assert not make(project).detect_completion()


def test_the_checkpoint_files_are_not_a_result(project):
    pipeline = make(project)
    results(pipeline, "pe_resume.pickle", "pe_dynesty.pickle")
    assert not pipeline.detect_completion()
    assert pipeline.collect_assets() == {}


def test_it_is_complete_when_the_result_exists(project):
    pipeline = make(project)
    results(pipeline, "pe_resume.pickle", "pe_result.hdf5")
    assert pipeline.detect_completion()
    assert pipeline.collect_assets() == {"samples": pipeline.result_file()}
    assert pipeline.result_file().endswith("/results/pe_result.hdf5")
    assert pipeline.samples() == pipeline.result_file()
    assert pipeline.output_paths() == {"samples": pipeline.result_file()}


def write_log(pipeline, *events):
    """The log HTCondor writes, from events: ("submitted",), ("exited", code), ("killed", n)."""
    os.makedirs(pipeline.production.rundir, exist_ok=True)
    path = os.path.join(pipeline.production.rundir, f"{pipeline.production.name}.log")
    with open(path, "w") as handle:
        for event in events:
            if event[0] == "submitted":
                handle.write(
                    "000 (1.000.000) 11/03 10:00:00 Job submitted from host: <1.2.3.4>\n...\n"
                )
            elif event[0] == "removed":
                handle.write("009 (1.000.000) 11/03 10:30:00 Job was aborted.\n...\n")
            else:
                how = (
                    f"(1) Normal termination (return value {event[1]})"
                    if event[0] == "exited"
                    else f"(0) Abnormal termination (signal {event[1]})"
                )
                handle.write(f"005 (1.000.000) 11/03 11:00:00 Job terminated.\n\t{how}\n...\n")


def write_error(pipeline, text):
    os.makedirs(pipeline.production.rundir, exist_ok=True)
    path = os.path.join(pipeline.production.rundir, f"{pipeline.production.name}.err")
    with open(path, "w") as handle:
        handle.write(text)


def resurrect(pipeline):
    """Resurrect it, and say whether it was submitted again."""
    with patch.object(PEPipeline, "submit_dag") as submit:
        pipeline.resurrect()
    return submit.called


def test_a_job_that_stopped_itself_is_submitted_again(project):
    pipeline = make(project)
    write_log(pipeline, ("submitted",), ("exited", 0), ("submitted",), ("exited", 77))
    assert resurrect(pipeline)


def test_a_job_that_was_submitted_again_is_running_and_is_not_submitted_a_third_time(project):
    # (The log still holds the end of the first run, which stopped itself.)
    pipeline = make(project)
    write_log(pipeline, ("submitted",), ("exited", 77), ("submitted",))
    assert not resurrect(pipeline)


@pytest.mark.parametrize(
    "events, said",
    [
        ((("submitted",), ("exited", 1)), "exited with code 1"),
        ((("submitted",), ("exited", 0)), "exited with code 0"),
        ((("submitted",), ("exited", 77), ("submitted",), ("exited", 2)), "exited with code 2"),
        ((("submitted",), ("killed", 9)), "killed by signal 9"),
        ((("submitted",), ("removed",)), "removed from the scheduler"),
    ],
)
def test_a_job_that_ended_without_a_result_is_a_failure(project, events, said):
    pipeline = make(project)
    write_log(pipeline, *events)
    with pytest.raises(CwinpyError, match=said) as caught:
        resurrect(pipeline)
    assert caught.value.transient is False


def test_a_job_whose_error_output_shows_an_exception_is_a_failure(project):
    pipeline = make(project)
    write_error(
        pipeline, "INFO starting\nTraceback (most recent call last):\n  File ...\nValueError: x\n"
    )
    with pytest.raises(CwinpyError, match="failed with an exception"):
        resurrect(pipeline)


def test_a_job_that_has_gone_with_no_sign_of_failure_is_left_alone(project):
    # The list of jobs may be out of date, or the scheduler in another process: it may be running.
    pipeline = make(project)
    assert not resurrect(pipeline)


def test_a_job_that_is_running_is_left_alone_though_it_has_a_log(project):
    pipeline = make(project)
    write_log(pipeline, ("submitted",))
    write_error(pipeline, "INFO sampling\n")
    assert not resurrect(pipeline)
