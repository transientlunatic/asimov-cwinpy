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


def sampler_kwargs(pipeline):
    """The ``sampler-kwargs`` of the configuration, as the dictionary that cwinpy reads."""
    import ast

    (found,) = [line for line in lines(pipeline) if line.startswith("sampler-kwargs = ")]
    return ast.literal_eval(found.split(" = ", 1)[1])


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
    pipeline = make(
        project,
        extra_analysis="sampler:\n  sampler: dynesty\n  sampler kwargs: {nlive: 100}\n",
    )
    config = lines(pipeline)
    assert sampler_kwargs(pipeline) == {"nlive": 100, "exit_code": 77}
    assert "sampler = dynesty" in config


def test_no_sampler_is_named_unless_the_analysis_names_one(project):
    assert not any(line.startswith("sampler = ") for line in lines(make(project)))


def test_the_sampler_stops_with_the_code_that_asimov_takes_for_a_restart(project):
    """bilby's own code for a checkpoint is 130, which asimov would take for a failure."""
    assert sampler_kwargs(make(project)) == {"exit_code": 77}


def test_an_exit_code_that_the_analysis_gives_is_kept(project):
    pipeline = make(
        project, extra_analysis="sampler:\n  sampler kwargs: {nlive: 50, exit_code: 5}\n"
    )
    assert sampler_kwargs(pipeline) == {"nlive": 50, "exit_code": 5}


def test_the_kwargs_of_the_analysis_are_not_changed_by_adding_the_code(project):
    pipeline = make(project, extra_analysis="sampler:\n  sampler kwargs: {nlive: 100}\n")
    lines(pipeline)
    assert pipeline.production.meta["sampler"]["sampler kwargs"] == {"nlive": 100}


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


# Without a shared filesystem: the files of the job are transferred


@pytest.fixture
def transferred(project, tmp_path):
    """A pipeline whose files are transferred, with files that exist, and a scheduler to watch."""
    data = {}
    for detector in ("H1", "L1"):
        path = tmp_path / "merged" / f"merged_J0534+2200_{detector}_2f.hdf5"
        path.parent.mkdir(exist_ok=True)
        path.write_text(detector)
        data[detector] = {2.0: str(path)}
    ephemeris = tmp_path / "J0534+2200.par"
    ephemeris.write_text("PSRJ J0534+2200\n")
    config = tmp_path / "repo" / "pe.ini"
    config.parent.mkdir()
    config.write_text("par-file = x\n")

    def build(extra_analysis="scheduler:\n  transfer files: true\n", **kwargs):
        pipeline = make(
            project,
            extra_analysis=extra_analysis,
            provided={DATA_ASSET: {"J0534+2200": data}},
            **kwargs,
        )
        pipeline.production.meta["ephemeris"] = str(ephemeris)
        pipeline.production.event.repository = MagicMock(directory=str(tmp_path / "repo"))
        pipeline.production.event.repository.find_prods.return_value = ["pe.ini"]
        pipeline.category = ""
        scheduler = MagicMock()
        scheduler.submit.return_value = 42
        with (
            patch.object(
                PEPipeline, "scheduler", new_callable=PropertyMock, return_value=scheduler
            ),
            patch("asimov_cwinpy.pe.shutil.which", return_value="/env/bin/cwinpy_pe"),
        ):
            yield_ = (pipeline, scheduler)
        return yield_

    return build, config, ephemeris, data


def submit(pipeline, scheduler, **kwargs):
    with (
        patch.object(PEPipeline, "scheduler", new_callable=PropertyMock, return_value=scheduler),
        patch("asimov_cwinpy.pe.shutil.which", return_value="/env/bin/cwinpy_pe"),
    ):
        pipeline.submit_dag(**kwargs)
    return scheduler.submit.call_args[0][0].kwargs if scheduler.submit.called else None


def test_the_files_are_only_transferred_if_the_ledger_says_so(project):
    pipeline = make(project)
    assert pipeline.transfers() is False
    pipeline.production.meta["scheduler"] = {"transfer files": True}
    assert pipeline.transfers() is True
    pipeline.production.meta["scheduler"] = {"transfer files": False}
    assert pipeline.transfers() is False


def test_every_path_in_the_configuration_is_then_the_name_of_a_file_in_the_directory_of_the_job(
    transferred,
):
    build, *_ = transferred
    pipeline, _ = build(extra_event="frequency factors: [2]\n")
    config = dict(line.split(" = ", 1) for line in lines(pipeline))
    assert config["par-file"] == "J0534+2200.par"
    assert (
        config["data-file-2f"]
        == "[H1:merged_J0534+2200_H1_2f.hdf5, L1:merged_J0534+2200_L1_2f.hdf5]"
    )
    assert config["prior"] == "pe.prior"
    assert config["outdir"] == "results"
    assert not any(os.sep in value for value in config.values())


def test_the_paths_are_absolute_when_nothing_is_transferred(project):
    config = dict(line.split(" = ", 1) for line in lines(make(project)))
    assert config["par-file"] == "/data/J0534+2200.par"
    assert config["data-file-2f"].startswith("[H1:/m/H1_2f.hdf5")
    assert os.path.isabs(config["outdir"]) and os.path.isabs(config["prior"])


def test_the_job_asks_htcondor_to_transfer_the_files_and_bring_the_results_back(transferred):
    build, config, ephemeris, data = transferred
    pipeline, scheduler = build()
    job = submit(pipeline, scheduler)
    rundir = pipeline.production.rundir
    assert job["should_transfer_files"] == "YES"
    assert job["when_to_transfer_output"] == "ON_EXIT_OR_EVICT"
    assert job["transfer_output_files"] == "results"
    assert job["initialdir"] == rundir  # so that results come back to <rundir>/results
    assert job["arguments"] == "--config pe.ini"
    sent = job["transfer_input_files"].split(", ")
    assert sent == [
        str(config),
        pipeline.prior_file(),
        str(ephemeris),
        data["H1"][2.0],
        data["L1"][2.0],
        os.path.join(rundir, "results"),
    ]


def test_the_directory_of_the_results_is_there_from_the_start_so_that_it_is_never_missing(
    transferred,
):
    build, *_ = transferred
    pipeline, scheduler = build()
    submit(pipeline, scheduler)
    assert os.path.isdir(os.path.join(pipeline.production.rundir, "results"))


def test_the_directory_of_the_results_is_not_made_when_nothing_is_transferred(project):
    pipeline = make(project)
    pipeline.production.event.repository = MagicMock(directory="/repo")
    pipeline.production.event.repository.find_prods.return_value = ["pe.ini"]
    pipeline.build_dag()
    assert not os.path.exists(os.path.join(pipeline.production.rundir, "results"))


def test_a_job_that_stopped_itself_gets_its_checkpoint_back(transferred):
    """It is the directory of the results that is sent, which holds the checkpoint."""
    build, *_ = transferred
    pipeline, scheduler = build(
        extra_analysis="scheduler:\n  transfer files: true\n  periodic restart time: 600\n"
    )
    submit(pipeline, scheduler)
    results = os.path.join(pipeline.production.rundir, "results")
    open(os.path.join(results, "pe_resume.pickle"), "w").write("checkpoint")
    job = submit(pipeline, scheduler)
    assert results in job["transfer_input_files"].split(", ")
    assert "periodic-restart-time = 600" in lines(pipeline)


def test_simulated_data_has_no_data_files_to_send(transferred):
    build, config, ephemeris, _ = transferred
    pipeline, scheduler = build(
        extra_analysis=SIMULATED + "scheduler:\n  transfer files: true\n", simulated=False
    )
    job = submit(pipeline, scheduler)
    sent = job["transfer_input_files"].split(", ")
    assert len(sent) == 4 and not any(path.endswith(".hdf5") for path in sent)


def test_two_files_with_one_name_are_refused_because_they_are_put_in_one_directory(transferred):
    build, config, ephemeris, data = transferred
    pipeline, scheduler = build()
    pipeline.production.meta["ephemeris"] = data["H1"][2.0].replace("H1", "L1")
    with pytest.raises(
        PipelineException, match="merged_J0534\\+2200_L1_2f.hdf5 is the name of more"
    ):
        submit(pipeline, scheduler)
    scheduler.submit.assert_not_called()


def test_a_file_that_is_not_there_is_an_error_here_and_not_a_job_on_hold(transferred):
    build, config, ephemeris, data = transferred
    pipeline, scheduler = build()
    os.remove(data["L1"][2.0])
    with pytest.raises(
        PipelineException, match="would hold the job.*merged_J0534\\+2200_L1_2f.hdf5"
    ):
        submit(pipeline, scheduler)
    scheduler.submit.assert_not_called()


def test_a_dry_run_does_not_need_the_files_to_be_there(transferred):
    build, config, ephemeris, data = transferred
    pipeline, scheduler = build()
    os.remove(data["L1"][2.0])
    assert pipeline.submit_dag(dryrun=True) is None
    scheduler.submit.assert_not_called()


def test_nothing_is_transferred_unless_asked_and_getenv_is_not_asked_for(submitting):
    pipeline, scheduler = submitting
    pipeline.submit_dag()
    job = scheduler.submit.call_args[0][0].kwargs
    assert "should_transfer_files" not in job and "transfer_input_files" not in job
    assert "getenv" not in job and "universe" not in job and "environment" not in job
    assert job["arguments"].startswith("--config /repo")


# What the ledger says of the software environment and of the jobs


def test_the_executable_is_in_the_software_environment_of_the_analysis(project, tmp_path):
    bin_directory = tmp_path / "igwn" / "bin"
    bin_directory.mkdir(parents=True)
    (bin_directory / "cwinpy_pe").write_text("#!/bin/sh\n")
    (bin_directory / "cwinpy_pe").chmod(0o755)
    pipeline = make(project, extra_analysis=f"scheduler:\n  environment: {tmp_path / 'igwn'}\n")
    assert pipeline._executable() == str(bin_directory / "cwinpy_pe")


def test_the_environment_of_the_analysis_is_used_rather_than_that_of_the_project(project, tmp_path):
    mine = tmp_path / "mine" / "bin"
    other = tmp_path / "other" / "bin"
    for directory in (mine, other):
        directory.mkdir(parents=True)
        (directory / "cwinpy_pe").write_text("#!/bin/sh\n")
        (directory / "cwinpy_pe").chmod(0o755)
    pipeline = make(project)
    with patch("asimov_cwinpy.jobs.config.get", return_value=str(tmp_path / "other")):
        assert pipeline._executable() == str(other / "cwinpy_pe")  # the project's
        pipeline.production.meta["scheduler"] = {"environment": str(tmp_path / "mine")}
        assert pipeline._executable() == str(mine / "cwinpy_pe")  # the analysis's


def test_what_the_ledger_says_of_the_job_goes_into_its_description(project):
    extra = (
        "scheduler:\n  environment variables:\n    LAL_DATA_PATH: /cvmfs/x\n    NOTE: two words\n"
        "htcondor:\n  universe: vanilla\n  requirements: (HAS_CVMFS =?= True)\n  getenv: true\n"
    )
    pipeline = make(project, extra_analysis=extra)
    pipeline.production.event.repository = MagicMock(directory="/repo")
    pipeline.production.event.repository.find_prods.return_value = ["pe.ini"]
    scheduler = MagicMock()
    scheduler.submit.return_value = 1
    job = submit(pipeline, scheduler)
    assert job["universe"] == "vanilla"
    assert job["requirements"] == "(HAS_CVMFS =?= True)"
    assert job["getenv"] == "true"
    assert job["environment"] == "\"LAL_DATA_PATH=/cvmfs/x NOTE='two words'\""


def test_an_environment_that_has_no_executable_is_an_error_and_not_another_from_the_path(
    project, tmp_path
):
    (tmp_path / "empty" / "bin").mkdir(parents=True)
    pipeline = make(project, extra_analysis=f"scheduler:\n  environment: {tmp_path / 'empty'}\n")
    with pytest.raises(PipelineException, match="Cannot find the cwinpy_pe executable .*empty"):
        pipeline._executable()


def test_with_nothing_chosen_the_one_on_the_path_is_used(project):
    pipeline = make(project)
    with patch("asimov_cwinpy.jobs.config.get", side_effect=KeyError("environment")):
        with patch("asimov_cwinpy.pe.shutil.which", return_value="/usr/bin/cwinpy_pe") as which:
            assert pipeline._executable() == "/usr/bin/cwinpy_pe"
    which.assert_called_once_with("cwinpy_pe")
