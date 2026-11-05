"""The cwinpy-heterodyne pipeline, without cwinpy or HTCondor."""

import ast
import configparser
import os
import sys
import types
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from asimov.pipeline import PipelineException

from asimov_cwinpy.common import CwinpyError, TransientCwinpyError
from asimov_cwinpy.heterodyne import ASSET, DAG_NAME, HeterodynePipeline

from .conftest import shard_blueprint


def make(project, **blueprint):
    project.apply(shard_blueprint(**blueprint))
    return project.analysis("O4a-heterodyne", "shard-0001").pipeline


def parsed(pipeline):
    """The configuration, read back as cwinpy would read it."""
    parser = configparser.ConfigParser(interpolation=None)
    parser.read_string(pipeline.render_config())
    return parser


def literal(parser, key, section="heterodyne"):
    return ast.literal_eval(parser.get(section, key))


@pytest.fixture(autouse=True)
def no_environment_capture():
    """Capturing the software environment runs package managers."""
    with patch.object(HeterodynePipeline, "_capture_environment"):
        yield


# The configuration


def test_the_settings_are_given_to_cwinpy_as_dictionaries_with_a_value_for_each_detector(project):
    config = parsed(make(project))
    assert literal(config, "detectors") == ["H1", "L1"]
    assert literal(config, "starttimes") == {"H1": 1000000000, "L1": 1000000000}
    assert literal(config, "endtimes") == {"H1": 1000002048, "L1": 1000002048}
    assert literal(config, "channels") == {"H1": "H1:FAKE-STRAIN", "L1": "L1:FAKE-STRAIN"}
    assert literal(config, "framecaches") == {"H1": "/frames/H1", "L1": "/frames/L1"}
    assert literal(config, "segmentlists") == {"H1": "/segs/H1.txt", "L1": "/segs/L1.txt"}
    assert literal(config, "pulsarfiles", "ephemerides") == ["/data/J0534+2200.par"]


def test_the_outputs_go_in_a_directory_for_each_detector_in_the_run_directory(project):
    pipeline = make(project)
    config = parsed(pipeline)
    assert literal(config, "outputdir") == {
        "H1": os.path.join(pipeline.production.rundir, "results", "H1"),
        "L1": os.path.join(pipeline.production.rundir, "results", "L1"),
    }
    assert config.get("run", "basedir") == pipeline.production.rundir
    assert config.get("heterodyne_dag", "submit") == os.path.join(
        pipeline.production.rundir, "submit"
    )


def test_the_dag_is_built_and_not_submitted_and_the_merge_is_left_to_its_own_analysis(project):
    config = parsed(make(project))
    assert config.get("heterodyne_dag", "build") == "True"
    assert config.get("heterodyne_dag", "submitdag") == "False"
    assert config.get("merge", "merge") == "False"


def test_a_job_that_cannot_read_a_frame_fails_unless_the_ledger_says_otherwise(project):
    assert parsed(make(project)).get("heterodyne", "strictdatarequirement") == "True"


def test_the_requirement_can_be_relaxed(project):
    pipeline = make(project, extra_analysis="  strict data requirement: false\n")
    assert parsed(pipeline).get("heterodyne", "strictdatarequirement") == "False"


def test_the_default_factor_is_twice_the_rotation_frequency_and_both_can_be_asked_for(project):
    assert literal(parsed(make(project)), "freqfactors") == [2]


def test_both_factors(project):
    pipeline = make(project, extra_event="frequency factors: [1, 2]\n")
    assert literal(parsed(pipeline), "freqfactors") == [1, 2]


def test_the_length_of_a_job_is_cwinpys_unless_given(project):
    assert not parsed(make(project)).has_option("heterodyne", "joblength")


def test_the_length_of_a_job(project):
    pipeline = make(project, extra_analysis="  joblength: 43200\n")
    assert parsed(pipeline).get("heterodyne", "joblength") == "43200"


def test_data_on_a_datafind_server(project):
    config = parsed(make(project, cache=False))
    assert literal(config, "frametypes") == {"H1": "H1_HOFT_C00", "L1": "L1_HOFT_C00"}
    assert literal(config, "includeflags") == {
        "H1": "H1:DMT-ANALYSIS_READY:1",
        "L1": "L1:DMT-ANALYSIS_READY:1",
    }
    assert config.get("heterodyne", "host") == "datafind.example.org:80"
    assert config.get("heterodyne", "segmentserver") == "https://segments.example.org"
    assert config.get("heterodyne_dag", "urltype") == "osdf"
    assert not config.has_option("heterodyne", "framecaches")


def test_data_in_caches_does_not_ask_the_datafind_server(project):
    config = parsed(make(project))
    for key in ("frametypes", "includeflags", "host", "segmentserver"):
        assert not config.has_option("heterodyne", key)


def test_the_resources_of_the_scheduler_are_the_resources_of_the_jobs(project):
    pipeline = make(
        project,
        extra_analysis=(
            "scheduler:\n  request memory: 16384MB\n  request disk: 40GB\n  request cpus: 2\n"
            "  accounting group: ligo.dev.o4.cw.targeted.bayesian\n"
        ),
    )
    dag = parsed(pipeline)["heterodyne_dag"]
    assert dag["request_memory"] == "16384MB"
    assert dag["request_disk"] == "40GB"
    assert dag["request_cpus"] == "2"
    assert dag["accounting_group"] == "ligo.dev.o4.cw.targeted.bayesian"


def test_values_with_a_percent_sign_are_not_interpolated(project):
    pipeline = make(project, ephemerides="[/data/50%.par]")
    assert literal(parsed(pipeline), "pulsarfiles", "ephemerides") == ["/data/50%.par"]


def test_asimov_makes_the_configuration_file_from_the_template(project, tmp_path):
    pipeline = make(project)
    target = tmp_path / "shard.ini"
    pipeline.production.make_config(str(target))
    text = target.read_text()
    assert pipeline.render_config() in text
    assert "created by asimov for O4a-heterodyne/shard-0001" in text


def test_the_end_must_be_after_the_start(project):
    pipeline = make(project)
    pipeline.production.meta["cwinpy-heterodyne"]["data end"] = 1000000000
    with pytest.raises(PipelineException, match="must be after"):
        pipeline.runner_config()


def test_a_factor_that_is_not_a_positive_number_is_an_error(project):
    pipeline = make(project, extra_event="frequency factors: [0]\n")
    with pytest.raises(PipelineException, match="positive numbers"):
        pipeline.runner_config()


def test_a_detector_without_a_value_is_an_error(project):
    pipeline = make(project)
    del pipeline.production.meta["data"]["channels"]["L1"]
    with pytest.raises(PipelineException, match="no `channels` for L1"):
        pipeline.runner_config()


def test_caches_need_segment_lists(project):
    pipeline = make(project)
    del pipeline.production.meta["cwinpy-heterodyne"]["segment lists"]
    with pytest.raises(PipelineException, match="no `segment lists` for H1, L1"):
        pipeline.runner_config()


def test_a_server_needs_frame_types_and_segments(project):
    pipeline = make(project, cache=False)
    del pipeline.production.meta["data"]["frame types"]
    with pytest.raises(PipelineException, match="no `frame types`"):
        pipeline.runner_config()


# Checking the ledger


def test_a_complete_analysis_passes_the_check(project, tmp_path):
    par = tmp_path / "J0534+2200.par"
    par.write_text("PSRJ J0534+2200\n")
    make(project, ephemerides=f"[{par}]").before_config()


def test_the_check_names_what_is_missing(project):
    project.apply(
        "kind: event\nname: O4a-heterodyne\n---\nkind: analysis\nname: shard-0001\n"
        "pipeline: cwinpy-heterodyne\nevent: O4a-heterodyne\n"
    )
    pipeline = project.analysis("O4a-heterodyne", "shard-0001").pipeline
    with pytest.raises(PipelineException) as caught:
        pipeline.before_config()
    for name in ("interferometers", "data start", "data end", "ephemerides", "channels"):
        assert name in str(caught.value)


def test_a_parameter_file_that_does_not_exist_fails_the_check(project):
    with pytest.raises(PipelineException, match="1 parameter file.*do not exist"):
        make(project).before_config()


def test_the_check_captures_the_environment(project, tmp_path):
    par = tmp_path / "x.par"
    par.write_text("PSRJ x\n")
    make(project, ephemerides=f"[{par}]").before_config()
    HeterodynePipeline._capture_environment.assert_called_once()


# Building the DAG


class FakeRunner:
    """What cwinpy's HeterodyneDAGRunner does that matters here, without cwinpy."""

    built = []
    observed_path = []

    def __init__(self, config):
        base = config.get("run", "basedir")
        submit = config.get("heterodyne_dag", "submit")
        os.makedirs(submit, exist_ok=True)
        self.dag_file = os.path.join(submit, DAG_NAME)
        open(self.dag_file, "w").close()
        FakeRunner.built.append(config)
        FakeRunner.observed_path.append(os.environ["PATH"])
        # cwinpy makes an empty file for each output when it builds the DAG
        self.heterodyned_files = {}
        for det in ("H1", "L1"):
            os.makedirs(os.path.join(base, "results", det), exist_ok=True)
            for factor in (1, 2):
                path = os.path.join(
                    base, "results", det, f"heterodyne_J0534+2200_{det}_{factor}.hdf5"
                )
                open(path, "w").close()
                self.heterodyned_files.setdefault(det, {})[factor] = {"J0534+2200": [path]}


@pytest.fixture
def fake_cwinpy(monkeypatch):
    FakeRunner.built.clear()
    FakeRunner.observed_path.clear()
    heterodyne = types.ModuleType("cwinpy.heterodyne.heterodyne")
    heterodyne.HeterodyneDAGRunner = FakeRunner
    for name, module in (
        ("cwinpy", types.ModuleType("cwinpy")),
        ("cwinpy.heterodyne", types.ModuleType("cwinpy.heterodyne")),
        ("cwinpy.heterodyne.heterodyne", heterodyne),
    ):
        monkeypatch.setitem(sys.modules, name, module)
    return FakeRunner


def configured(project, tmp_path, **blueprint):
    """A pipeline that can be built: its configuration is in a repository."""
    par = tmp_path / "J0534+2200.par"
    par.write_text("PSRJ J0534+2200\n")
    pipeline = make(project, ephemerides=f"[{par}]", **blueprint)
    config = tmp_path / "shard-0001.ini"
    config.write_text(pipeline.render_config())
    repository = MagicMock(directory=str(tmp_path))
    repository.find_prods.return_value = [config.name]
    pipeline.production.event.repository = repository
    pipeline.category = ""
    return pipeline


def test_building_makes_the_dag_with_cwinpy_and_writes_down_what_to_expect(
    project, tmp_path, fake_cwinpy
):
    pipeline = configured(project, tmp_path)
    dag = pipeline.build_dag()
    assert dag == pipeline.dag_file() and os.path.exists(dag)
    assert len(pipeline.expected_files()) == 4
    assert all(
        os.path.dirname(f).startswith(pipeline.production.rundir) for f in pipeline.expected_files()
    )
    assert pipeline.manifest()["dag"] == dag


def test_cwinpy_is_given_the_configuration_asimov_made(project, tmp_path, fake_cwinpy):
    pipeline = configured(project, tmp_path)
    pipeline.build_dag()
    config = fake_cwinpy.built[0]
    assert literal(config, "detectors") == ["H1", "L1"]
    assert config.has_option("heterodyne", "framecaches")


def test_a_dag_that_has_been_built_is_not_built_again(project, tmp_path, fake_cwinpy):
    pipeline = configured(project, tmp_path)
    pipeline.build_dag()
    pipeline.build_dag()
    assert len(fake_cwinpy.built) == 1


def test_cwinpy_finds_its_executables_in_the_environment_of_the_pipelines(
    project, tmp_path, fake_cwinpy
):
    environment = tmp_path / "environment"
    (environment / "bin").mkdir(parents=True)
    pipeline = configured(project, tmp_path)
    before = os.environ["PATH"]
    with patch("asimov_cwinpy.heterodyne.config.get", return_value=str(environment)):
        pipeline.build_dag()
    assert fake_cwinpy.observed_path[0].startswith(str(environment / "bin"))
    assert os.environ["PATH"] == before


def test_a_dry_run_builds_nothing(project, tmp_path, fake_cwinpy):
    pipeline = configured(project, tmp_path)
    pipeline.build_dag(dryrun=True)
    assert not fake_cwinpy.built and pipeline.manifest() is None


def test_building_without_cwinpy_says_so(project, tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "cwinpy.heterodyne.heterodyne", None)
    with pytest.raises(PipelineException, match="cwinpy must be installed"):
        configured(project, tmp_path).build_dag()


def test_a_problem_cwinpy_finds_is_reported(project, tmp_path, fake_cwinpy):
    pipeline = configured(project, tmp_path)
    with patch.object(FakeRunner, "__init__", side_effect=ValueError("No science data segments")):
        with pytest.raises(PipelineException, match="cwinpy could not make the DAG.*No science"):
            pipeline.build_dag()


def test_building_without_a_configuration_is_an_error(project, tmp_path, fake_cwinpy):
    pipeline = configured(project, tmp_path)
    pipeline.production.event.repository.find_prods.return_value = []
    with pytest.raises(PipelineException, match="No configuration file found"):
        pipeline.build_dag()


# Submitting


class Scheduler:
    """A scheduler that can submit a DAG again."""

    def __init__(self):
        self.calls = []

    def submit_dag(self, dag_file, batch_name=None, force=False, **kwargs):
        self.calls.append({"dag": dag_file, "batch": batch_name, "force": force, **kwargs})
        return 1000 + len(self.calls)


class OldScheduler:
    """A scheduler from before a DAG could be submitted again (asimov #314)."""

    def submit_dag(self, dag_file, batch_name=None, **kwargs):
        return 7


def submitting(project, tmp_path, fake_cwinpy, scheduler=None):
    pipeline = configured(project, tmp_path)
    scheduler = scheduler or Scheduler()
    patcher = patch.object(
        HeterodynePipeline, "scheduler", new_callable=PropertyMock, return_value=scheduler
    )
    patcher.start()
    return pipeline, scheduler, patcher


@pytest.fixture
def submit(project, tmp_path, fake_cwinpy):
    started = []

    def make_one(scheduler=None):
        pipeline, scheduler, patcher = submitting(project, tmp_path, fake_cwinpy, scheduler)
        started.append(patcher)
        return pipeline, scheduler

    yield make_one
    for patcher in started:
        patcher.stop()


def test_the_dag_is_submitted(submit):
    pipeline, scheduler = submit()
    assert pipeline.submit_dag() == 1001
    call = scheduler.calls[0]
    assert call["dag"] == pipeline.dag_file()
    assert call["batch"] == "cwinpy-heterodyne/O4a-heterodyne/shard-0001"
    assert call["force"] is False
    assert pipeline.production.status == "running"
    assert pipeline.production.job_id == 1001


def test_a_dag_that_has_run_is_submitted_again_so_that_dagman_resumes_it(submit):
    pipeline, scheduler = submit()
    pipeline.submit_dag()
    for ending in (".dagman.out", ".condor.sub"):
        open(pipeline.dag_file() + ending, "w").write("EXITING WITH STATUS 1\n")
    assert pipeline.submit_dag() == 1002
    assert scheduler.calls[1]["force"] is True
    assert len(scheduler.calls) == 2


def test_the_log_of_the_last_run_is_kept_but_is_not_taken_for_the_next(submit):
    pipeline, _ = submit()
    pipeline.submit_dag()
    log = pipeline.dag_file() + ".dagman.out"
    open(log, "w").write("EXITING WITH STATUS 1\n")
    pipeline.submit_dag()
    assert not os.path.exists(log)
    assert open(log + ".1").read() == "EXITING WITH STATUS 1\n"
    assert pipeline.dag_outcome() is None
    open(log, "w").write("EXITING WITH STATUS 1\n")
    pipeline.submit_dag()
    assert os.path.exists(log + ".2")


def test_a_scheduler_that_cannot_submit_again_is_an_error_but_can_submit_the_first_time(submit):
    pipeline, _ = submit(OldScheduler())
    assert pipeline.submit_dag() == 7
    open(pipeline.dag_file() + ".dagman.out", "w").close()
    with pytest.raises(PipelineException, match="etive-io/asimov#314"):
        pipeline.submit_dag()


@pytest.mark.parametrize("error", [RuntimeError("no schedd"), NotImplementedError("no dags")])
def test_a_scheduler_that_fails_is_a_pipeline_error(submit, error):
    pipeline, scheduler = submit()
    scheduler.submit_dag = MagicMock(side_effect=error)
    with pytest.raises(PipelineException, match="could not be submitted"):
        pipeline.submit_dag()


def test_a_dry_run_submits_nothing(submit):
    pipeline, scheduler = submit()
    assert pipeline.submit_dag(dryrun=True) is None
    assert scheduler.calls == []


# Knowing how it went


@pytest.fixture
def built(project, tmp_path, fake_cwinpy):
    pipeline = configured(project, tmp_path)
    pipeline.build_dag()
    return pipeline


def dagman(pipeline, status=None, text=""):
    with open(pipeline.dag_file() + ".dagman.out", "w") as handle:
        handle.write(text)
        if status is not None:
            handle.write(f"11/03/26 10:00:00 ... EXITING WITH STATUS {status}\n")


def fill(pipeline, only=None):
    for path in pipeline.expected_files()[:only]:
        open(path, "w").write("data")


def node_error(pipeline, name, text):
    os.makedirs(os.path.join(pipeline.production.rundir, "log"), exist_ok=True)
    with open(os.path.join(pipeline.production.rundir, "log", f"{name}.err"), "w") as handle:
        handle.write(text)


def test_it_is_not_complete_before_it_is_built(project):
    assert not make(project).detect_completion()


def test_it_is_not_complete_before_dagman_has_finished(built):
    fill(built)
    assert not built.detect_completion()
    dagman(built, status=None, text="running...\n")
    assert not built.detect_completion()


def test_the_empty_files_cwinpy_makes_when_it_builds_the_dag_are_not_a_result(built):
    dagman(built, 0)
    assert not built.detect_completion()
    assert len(built.incomplete_outputs()) == 4


def test_it_is_complete_when_the_dag_succeeded_and_every_output_has_data(built):
    fill(built)
    dagman(built, 0)
    assert built.detect_completion()


def test_one_output_without_data_is_not_complete(built):
    fill(built, only=3)
    dagman(built, 0)
    assert not built.detect_completion()
    assert len(built.incomplete_outputs()) == 1


def test_a_missing_output_is_not_complete(built):
    fill(built)
    os.remove(built.expected_files()[0])
    dagman(built, 0)
    assert not built.detect_completion()


def test_a_dag_that_ended_with_failed_jobs_is_not_complete_though_the_files_are_there(built):
    fill(built)
    dagman(built, 2)
    assert not built.detect_completion()


def test_what_counts_is_how_the_last_run_ended(built):
    fill(built)
    dagman(
        built, text="EXITING WITH STATUS 1\nresumed from the rescue file\nEXITING WITH STATUS 0\n"
    )
    assert built.dag_outcome() == "succeeded"
    assert built.detect_completion()


def test_only_the_end_of_a_long_log_is_read(built):
    dagman(built, text="x" * 500_000 + "\n")
    with open(built.dag_file() + ".dagman.out", "a") as handle:
        handle.write("EXITING WITH STATUS 0\n")
    assert built.dag_outcome() == "succeeded"


# Resurrecting


def test_a_dag_that_has_not_ended_is_left_alone(built):
    built.resurrect()


def test_a_dag_that_ended_well_is_left_alone(built):
    fill(built)
    dagman(built, 0)
    built.resurrect()


@pytest.mark.parametrize(
    "text",
    [
        "OSError: Failed to read frame data from cache",
        "XLAL Error - XLALFrFileOpenURL (LALFrameIO.c:121): Could not open TOC for x.gwf",
        "XLAL Error - XLALFrStreamCacheOpen (LALFrStream.c:231): failed: I/O error",
        "OSError: [Errno 116] Stale file handle",
    ],
)
def test_jobs_that_could_not_read_frame_files_are_a_transient_failure(built, text):
    dagman(built, 1)
    node_error(built, "cwinpy_heterodyne_0_2_H1_1000000000-1000002048", "INFO ok\n" + text + "\n")
    node_error(built, "cwinpy_heterodyne_0_2_L1_1000000000-1000002048", "INFO ok\n")
    with pytest.raises(TransientCwinpyError, match="1 job.*could not read frame files") as caught:
        built.resurrect()
    assert caught.value.transient is True
    assert "H1" in str(caught.value)


def test_a_dag_that_failed_for_another_reason_is_not_transient(built):
    dagman(built, 1)
    node_error(
        built, "cwinpy_heterodyne_0_2_H1_1000000000-1000002048", "ValueError: bad ephemeris\n"
    )
    with pytest.raises(CwinpyError, match="ended with failed jobs") as caught:
        built.resurrect()
    assert caught.value.transient is False


def test_a_dag_that_failed_with_no_logs_is_not_transient(built):
    dagman(built, 1)
    with pytest.raises(CwinpyError) as caught:
        built.resurrect()
    assert caught.value.transient is False


def test_a_dag_that_succeeded_with_empty_outputs_is_a_failure_which_names_the_cause(built):
    fill(built, only=2)
    dagman(built, 0)
    with pytest.raises(
        CwinpyError, match="2 of its outputs are missing or empty.*strict data"
    ) as caught:
        built.resurrect()
    assert caught.value.transient is False


# What it provides


def test_there_are_no_assets_until_it_is_complete(built):
    assert built.collect_assets() == {}


def test_the_assets_are_the_files_by_pulsar_detector_and_factor(built):
    fill(built)
    dagman(built, 0)
    files = built.collect_assets()[ASSET]
    assert sorted(files) == ["J0534+2200"]
    assert sorted(files["J0534+2200"]) == ["H1", "L1"]
    assert sorted(files["J0534+2200"]["H1"]) == [1, 2]
    assert files["J0534+2200"]["H1"][2][0].endswith("heterodyne_J0534+2200_H1_2.hdf5")


def test_the_outputs_are_listed_for_asimov(built):
    assert sorted(built.output_paths()[ASSET]) == sorted(built.expected_files())


def test_the_log_to_report_is_the_dagman_log(built):
    dagman(built, 0, text="started\n")
    assert list(built.collect_logs()) == [DAG_NAME + ".dagman.out"]
