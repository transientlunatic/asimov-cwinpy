"""The cwinpy-heterodyne pipeline, without cwinpy or HTCondor."""

import ast
import configparser
import os
import sys
import time
import types
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from asimov.pipeline import PipelineException

from asimov_cwinpy.common import CwinpyError, TransientCwinpyError
from asimov_cwinpy.heterodyne import (
    ASSET,
    DAG_NAME,
    HeterodynePipeline,
    describe_progress,
    parse_progress,
)

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


# Progress

#: The table of counts as DAGMan writes it to its log, as it is believed to (from memory of
#: HTCondor's logs: this has not been checked against a pool, see PLAN.md).
TABLE = """\
11/11/26 10:00:00 Of {total} nodes total:
11/11/26 10:00:00  Done     Pre   Queued    Post   Ready   Un-Ready   Failed
11/11/26 10:00:00   ===     ===      ===     ===     ===        ===      ===
11/11/26 10:00:00 {done:>5}{pre:>8}{queued:>9}{post:>8}{ready:>8}{waiting:>11}{failed:>9}
"""


def table(total=2000, done=120, pre=0, queued=20, post=0, ready=1860, waiting=0, failed=0):
    return TABLE.format(**locals())


def test_the_counts_are_read_from_the_table_in_the_log():
    assert parse_progress(table()) == {
        "total": 2000,
        "done": 120,
        "pre": 0,
        "queued": 20,
        "post": 0,
        "ready": 1860,
        "waiting": 0,
        "failed": 0,
    }


def test_every_column_is_its_own():
    found = parse_progress(
        table(total=28, done=1, pre=2, queued=3, post=4, ready=5, waiting=6, failed=7)
    )
    assert found == {
        "total": 28,
        "done": 1,
        "pre": 2,
        "queued": 3,
        "post": 4,
        "ready": 5,
        "waiting": 6,
        "failed": 7,
    }


def test_the_latest_table_is_the_one_that_counts():
    text = table(done=1) + "11/11/26 10:01:00 other lines\n" * 3 + table(done=2) + "...\n"
    assert parse_progress(text)["done"] == 2


def test_the_total_is_the_one_before_the_table_that_is_used():
    assert (
        parse_progress(
            table(total=10, done=5, queued=0, ready=5) + table(total=12, done=6, queued=0, ready=6)
        )["total"]
        == 12
    )


def test_without_the_total_the_nodes_are_added_up():
    text = table().split("\n", 1)[1]
    assert parse_progress(text)["total"] == 2000


def test_a_log_without_a_table_has_no_progress():
    assert parse_progress("") is None
    assert parse_progress("11/11/26 10:00:00 Submitting node A\n") is None


def test_a_table_cut_off_before_its_numbers_has_no_progress():
    assert parse_progress(table().rsplit("\n", 2)[0] + "\n") is None
    assert parse_progress(table()[: table().index("===")]) is None


def test_other_lines_after_the_table_do_not_matter():
    assert (
        parse_progress(table() + "11/11/26 10:00:01 Submitting HTCondor Node x job(s)\n")["done"]
        == 120
    )


def test_progress_is_described_for_the_monitor():
    assert (
        describe_progress(parse_progress(table()))
        == "120 of 2000 jobs done (6%), 20 queued or running"
    )


def test_the_percentage_is_rounded_down_so_that_it_is_not_done_before_it_is():
    assert "(6%)" in describe_progress(parse_progress(table(done=121)))
    assert "(99%)" in describe_progress(
        parse_progress(table(total=1000, done=999, ready=1, queued=0))
    )


def test_what_is_waiting_to_start_or_finish_counts_as_running():
    line = describe_progress(parse_progress(table(pre=1, queued=2, post=3)))
    assert "6 queued or running" in line


def test_a_failure_is_in_the_description():
    assert "3 failed" in describe_progress(parse_progress(table(failed=3)))
    assert "failed" not in describe_progress(parse_progress(table()))


def test_a_dag_with_no_nodes_has_a_description():
    line = describe_progress(parse_progress(table(total=0, done=0, queued=0, ready=0)))
    assert line.startswith("0 of 0 jobs done")


def test_the_progress_of_a_dag_is_read_from_its_log(built):
    assert built.dag_progress() is None  # DAGMan has not written
    dagman(built, text="some log\n")
    assert built.dag_progress() is None
    dagman(built, text=table(done=7))
    assert built.dag_progress()["done"] == 7


def test_the_table_is_found_at_the_end_of_a_long_log(built):
    dagman(built, text="x" * 500_000 + "\n" + table(done=9))
    assert built.dag_progress()["done"] == 9


def test_only_the_end_of_the_log_is_read_for_progress(built):
    # (a table from long ago is not the state of the DAG now)
    dagman(built, text=table(done=1) + "x" * 500_000 + "\n")
    assert built.dag_progress() is None


def test_while_running_says_how_far_it_has_got_and_keeps_it_in_the_ledger(built, capsys):
    dagman(built, text=table(done=120, failed=2))
    built.while_running()
    said = capsys.readouterr().out
    assert "120 of 2000 jobs done (6%)" in said and "2 failed" in said
    kept = built.production.meta["progress"]
    assert kept["done"] == 120 and kept["failed"] == 2 and kept["total"] == 2000
    assert abs(kept["read"] - time.time()) < 60


def test_while_running_keeps_the_latest_counts(built):
    dagman(built, text=table(done=1))
    built.while_running()
    dagman(built, text=table(done=2))
    built.while_running()
    assert built.production.meta["progress"]["done"] == 2


def test_while_running_before_dagman_has_said_anything_does_nothing(built, capsys):
    built.while_running()
    dagman(built, text="")
    built.while_running()
    assert capsys.readouterr().out == ""
    assert "progress" not in built.production.meta


def test_while_running_does_not_touch_the_outcome(built):
    dagman(built, text=table(done=3))
    built.while_running()
    assert built.dag_outcome() is None
    assert not built.detect_completion()


def test_the_monitor_calls_it_for_a_dag_that_is_running(built, capsys):
    from asimov.monitor_states import RunningState

    dagman(built, text=table(done=40, total=100, ready=60, queued=0))
    context = MagicMock()
    context.analysis = built.production
    context.job = {"status": 2}  # HTCondor's "running"
    context.has_condor_job.return_value = True
    assert RunningState().handle(context) is True
    said = capsys.readouterr().out
    assert "40 of 100 jobs done (40%)" in said
    assert built.production.meta["progress"]["done"] == 40
    context.update_ledger.assert_called()


# Coverage

SHARD = (1000000000, 1000002048)  # the span of the shard in the blueprint


def cover(pipeline, tmp_path, science, heterodyned, minimum=None):
    """
    Give a built shard science segments, and outputs that hold data with their own segments.

    ``science`` and ``heterodyned`` are ``{detector: segments}``.
    """
    h5py = pytest.importorskip("h5py")
    settings = pipeline.production.meta["cwinpy-heterodyne"]
    lists = {}
    for detector, segments in science.items():
        path = tmp_path / f"science_{detector}.txt"
        path.write_text("".join(f"{a} {b}\n" for a, b in segments))
        lists[detector] = str(path)
    settings["segment lists"] = lists
    if minimum is not None:
        settings["minimum coverage"] = minimum
    for record in pipeline.manifest()["outputs"]:
        for path in record["files"]:
            with h5py.File(path, "w") as handle:
                handle.create_dataset(
                    "HeterodynedDataTimeSegments", data=heterodyned[record["detector"]]
                )
    dagman(pipeline, 0)


FULL = [(SHARD[0], SHARD[1])]


def test_a_shard_with_all_its_science_time_heterodyned_is_complete(built, tmp_path):
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": FULL, "L1": FULL}, minimum=0.9)
    assert built.detect_completion()
    built.resurrect()  # does not raise


def test_a_detector_with_too_little_is_not_complete_and_says_which(built, tmp_path):
    ignored = [(SHARD[0], SHARD[0] + 1000)]  # 1000 of 2048 s
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": FULL, "L1": ignored}, minimum=0.9)
    assert not built.detect_completion()
    assert list(built.low_coverage()) == ["L1"]
    with pytest.raises(CwinpyError, match=r"minimum coverage.*90.0%.*L1 48.8%") as error:
        built.resurrect()
    assert "H1" not in str(error.value).split("L1")[0].split(":")[-1]
    assert not isinstance(error.value, TransientCwinpyError)
    assert error.value.transient is False


def test_both_detectors_are_named_when_both_are_low(built, tmp_path):
    half = [(SHARD[0], SHARD[0] + 1024)]
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": half, "L1": half}, minimum=0.9)
    with pytest.raises(CwinpyError, match=r"H1 50.0%, L1 50.0%"):
        built.resurrect()


def test_exactly_the_minimum_is_enough(built, tmp_path):
    half = [(SHARD[0], SHARD[0] + 1024)]
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": half, "L1": half}, minimum=0.5)
    assert built.detect_completion()


def test_without_a_minimum_nothing_is_checked(built, tmp_path):
    tiny = [(SHARD[0], SHARD[0] + 10)]
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": tiny, "L1": tiny})
    assert built.detect_completion()
    assert built.low_coverage() == {}


def test_science_time_is_only_that_inside_the_span_of_the_shard(built, tmp_path):
    wide = [(SHARD[0] - 5000, SHARD[1] + 5000)]  # the flag is up for much longer
    cover(built, tmp_path, {"H1": wide, "L1": wide}, {"H1": FULL, "L1": FULL}, minimum=0.99)
    assert built.detect_completion()
    assert built.coverage()["H1"]["science"] == SHARD[1] - SHARD[0]


def test_a_gap_in_the_science_time_is_not_data_that_was_ignored(built, tmp_path):
    science = [(SHARD[0], SHARD[0] + 1000), (SHARD[0] + 1500, SHARD[1])]
    heterodyned = [(SHARD[0] + 10, SHARD[0] + 1000), (SHARD[0] + 1500, SHARD[1] - 10)]
    cover(
        built,
        tmp_path,
        {"H1": science, "L1": science},
        {"H1": heterodyned, "L1": heterodyned},
        minimum=0.95,
    )
    assert built.detect_completion()
    assert built.coverage()["H1"]["science"] == 1548


def test_a_detector_with_no_science_time_is_left_out(built, tmp_path):
    cover(built, tmp_path, {"H1": FULL, "L1": []}, {"H1": FULL, "L1": FULL}, minimum=0.9)
    assert sorted(built.coverage()) == ["H1"]
    assert built.detect_completion()


def test_the_coverage_cannot_be_measured_without_the_segments_so_it_is_not_checked(built, tmp_path):
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": [(0, 1)], "L1": [(0, 1)]}, minimum=0.9)
    built.production.meta["cwinpy-heterodyne"]["segment lists"] = {
        "H1": str(tmp_path / "gone.txt"),
        "L1": str(tmp_path / "gone.txt"),
    }
    assert built.coverage() is None
    assert built.detect_completion()


def test_an_output_that_does_not_say_what_it_covers_is_not_checked(built, tmp_path):
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": FULL, "L1": FULL}, minimum=0.9)
    for path in built.expected_files():
        open(path, "w").write("data")  # not an HDF5 file
    assert built.coverage() is None
    assert built.detect_completion()


def test_what_is_measured_is_one_pulsars_files_for_one_factor(built, tmp_path):
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": FULL, "L1": FULL}, minimum=0.9)
    factor_two = [r for r in built.manifest()["outputs"] if r["factor"] == 2]
    for record in factor_two:  # the other factor's files are not what is measured
        for path in record["files"]:
            open(path, "w").write("data")
    assert built.coverage()["H1"]["fraction"] == 1.0
    assert built.detect_completion()


def test_low_coverage_is_not_asked_before_the_dag_has_succeeded(built, tmp_path):
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": [(0, 1)], "L1": [(0, 1)]}, minimum=0.9)
    dagman(built)  # DAGMan has not finished
    assert not built.detect_completion()
    built.resurrect()  # nothing says that it failed
    dagman(built, 1)
    with pytest.raises(CwinpyError, match="ended with failed jobs"):
        built.resurrect()


def test_empty_outputs_are_still_an_error_before_coverage_is_asked(built, tmp_path):
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": FULL, "L1": FULL}, minimum=0.9)
    open(built.expected_files()[0], "w").close()
    with pytest.raises(CwinpyError, match="missing or empty"):
        built.resurrect()


def test_the_science_segments_of_a_flag_are_the_files_that_cwinpy_wrote(project, tmp_path):
    pipeline = make(project, cache=False)
    directory = os.path.join(pipeline.production.rundir, "segments")
    os.makedirs(directory)
    for name, text in {
        "segments_1000000000-1000002048_H1_DMT-ANALYSIS_READY_1.txt": "1000000000 1000001000\n",
        "segments_1000000000-1000002048_L1_DMT-ANALYSIS_READY_1.txt": "1000000500 1000002048\n",
    }.items():
        with open(os.path.join(directory, name), "w") as handle:
            handle.write(text)
    assert pipeline.science_segments("H1") == [(1000000000, 1000001000)]
    assert pipeline.science_segments("L1") == [(1000000500, 1000002048)]


def test_the_segments_of_a_flag_are_not_there_before_the_dag_is_made(project):
    assert make(project, cache=False).science_segments("H1") is None


def test_a_detector_that_has_no_segment_file_is_not_measured_in_flag_mode(project, tmp_path):
    pipeline = make(project, cache=False)
    directory = os.path.join(pipeline.production.rundir, "segments")
    os.makedirs(directory)
    open(os.path.join(directory, "segments_1-2_H1_FLAG.txt"), "w").write("1000000000 1000001000\n")
    assert pipeline.science_segments("L1") is None
    assert pipeline.science_segments("H1") is not None


@pytest.mark.parametrize("value", [0, -0.5, 1.5, "most", True, [0.9]])
def test_a_minimum_that_is_not_a_fraction_is_refused(project, value):
    pipeline = make(project)
    pipeline.production.meta["cwinpy-heterodyne"]["minimum coverage"] = value
    with pytest.raises(PipelineException, match="minimum coverage"):
        pipeline.before_config()


@pytest.mark.parametrize("value", [1, 1.0, 0.9, 0.01, None])
def test_a_minimum_that_is_a_fraction_is_accepted(project, tmp_path, value):
    pipeline = configured(project, tmp_path)
    if value is not None:
        pipeline.production.meta["cwinpy-heterodyne"]["minimum coverage"] = value
    pipeline.before_config()


def test_the_coverage_is_kept_in_the_ledger_when_the_shard_finishes(built, tmp_path):
    ignored = [(SHARD[0], SHARD[0] + 1024)]
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": FULL, "L1": ignored})  # no minimum
    built.after_completion()
    kept = built.production.meta["coverage"]
    assert kept["H1"] == {"science": 2048, "heterodyned": 2048, "fraction": 1.0}
    assert kept["L1"] == {"science": 2048, "heterodyned": 1024, "fraction": 0.5}
    assert str(built.production.status).lower() == "finished"


def test_a_shard_finishes_even_if_the_coverage_cannot_be_measured(built, tmp_path):
    cover(built, tmp_path, {"H1": FULL, "L1": FULL}, {"H1": FULL, "L1": FULL})
    for path in built.expected_files():
        open(path, "w").write("data")
    built.after_completion()
    assert "coverage" not in built.production.meta
    assert str(built.production.status).lower() == "finished"


def test_the_per_chunk_files_may_be_released_once_the_merge_has_used_them():
    assert HeterodynePipeline.output_retention == {ASSET: "until_consumed"}
    assert HeterodynePipeline.output_retention[ASSET] == "until_consumed"
