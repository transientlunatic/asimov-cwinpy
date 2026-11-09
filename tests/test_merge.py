"""The cwinpy-merge pipeline, without cwinpy or HTCondor."""

import json
import os
import sys
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from asimov.pipeline import PipelineException

from asimov_cwinpy.common import CwinpyError, TransientCwinpyError
from asimov_cwinpy.heterodyne import ASSET as SHARD_ASSET
from asimov_cwinpy.heterodyne import HeterodynePipeline
from asimov_cwinpy.merge import ASSET, MergePipeline, by_time, number


def shard_file(det, factor, start, end, pulsar="J0534+2200", where="r"):
    return f"/{where}/{det}/heterodyne_{pulsar}_{det}_{factor}_{start}-{end}.hdf5"


def shard_assets(
    start, end, detectors=("H1", "L1"), factors=(2,), pulsars=("J0534+2200",), where="r"
):
    return {
        pulsar: {
            det: {
                factor: [shard_file(det, factor, start, end, pulsar, where)] for factor in factors
            }
            for det in detectors
        }
        for pulsar in pulsars
    }


# The first shard's files sort after the second's by name, so that putting them in the
# order of their names would put the later data first.
FIRST = shard_assets(1000000000, 1000002048, where="zz")
SECOND = shard_assets(1000002048, 1000004096, where="aa")


def blueprint(shards=("shard-0001", "shard-0002"), extra="", other=False):
    text = "kind: event\nname: O4a-heterodyne\ninterferometers: [H1, L1]\n"
    for name in shards:
        text += (
            f"---\nkind: analysis\nname: {name}\npipeline: cwinpy-heterodyne\n"
            "event: O4a-heterodyne\n"
        )
    needs = list(shards)
    if other:  # an analysis which is not a shard, which the merge also needs
        text += "---\nkind: analysis\nname: other\npipeline: cwinpy-pe\nevent: O4a-heterodyne\n"
        needs.append("other")
    text += (
        "---\nkind: analysis\nname: merge\npipeline: cwinpy-merge\nevent: O4a-heterodyne\n"
        f"needs: [{', '.join(needs)}]\n{extra}"
    )
    return text


@pytest.fixture
def provided():
    """What each shard provides, by name; a shard not in it is not complete."""
    assets = {"shard-0001": FIRST, "shard-0002": SECOND}

    def collect(self):
        files = assets.get(self.production.name)
        return {SHARD_ASSET: files} if files else {}

    with patch.object(HeterodynePipeline, "collect_assets", collect):
        yield assets


def make(project, **kwargs):
    project.apply(blueprint(**kwargs))
    return project.analysis("O4a-heterodyne", "merge").pipeline


@pytest.fixture(autouse=True)
def no_environment_capture():
    with patch.object(MergePipeline, "_capture_environment"):
        yield


# What it merges


def test_the_files_of_the_shards_are_joined_for_each_pulsar_detector_and_factor(project, provided):
    items = make(project).items()
    assert [(i["pulsar"], i["detector"], i["factor"]) for i in items] == [
        ("J0534+2200", "H1", 2),
        ("J0534+2200", "L1", 2),
    ]
    assert items[0]["inputs"] == [
        shard_file("H1", 2, 1000000000, 1000002048, where="zz"),
        shard_file("H1", 2, 1000002048, 1000004096, where="aa"),
    ]


def test_the_files_are_in_the_order_of_the_data_whatever_the_order_of_the_shards(project, provided):
    provided["shard-0001"], provided["shard-0002"] = SECOND, FIRST
    inputs = make(project).items()[0]["inputs"]
    assert [os.path.basename(f).split("_")[-1] for f in inputs] == [
        "1000000000-1000002048.hdf5",
        "1000002048-1000004096.hdf5",
    ]


def test_each_factor_and_each_pulsar_is_its_own_merge(project, provided):
    provided["shard-0001"] = shard_assets(
        1000000000, 1000002048, factors=(1, 2), pulsars=("J0534+2200", "J0537-6910")
    )
    provided["shard-0002"] = shard_assets(
        1000002048, 1000004096, factors=(1, 2), pulsars=("J0534+2200", "J0537-6910")
    )
    items = make(project).items()
    assert len(items) == 8  # two pulsars, two detectors, two factors
    assert {i["factor"] for i in items} == {1, 2}


def test_the_factor_is_a_whole_number_whether_it_is_written_2_or_2_point_0(project, provided):
    """The name of the merged file and the plan say 2, not 2.0, whichever shard said which."""
    for name, assets in (("shard-0001", FIRST), ("shard-0002", SECOND)):
        provided[name] = {"J0534+2200": {d: {2.0: v[2]} for d, v in assets["J0534+2200"].items()}}
    items = make(project).items()
    assert len(items) == 2 and all(len(i["inputs"]) == 2 for i in items)
    assert all(i["factor"] == 2 and isinstance(i["factor"], int) for i in items)
    assert items[0]["output"].endswith("merged_J0534+2200_H1_2f.hdf5")
    assert number(2.0) == 2 and number(1.5) == 1.5


def test_a_detector_that_only_one_shard_has_is_merged_from_that_shard(project, provided):
    provided["shard-0002"] = shard_assets(1000002048, 1000004096, detectors=("H1",))
    by_detector = {i["detector"]: i for i in make(project).items()}
    assert len(by_detector["H1"]["inputs"]) == 2
    assert len(by_detector["L1"]["inputs"]) == 1


def test_each_merged_file_is_in_a_directory_for_its_detector(project, provided):
    pipeline = make(project)
    item = pipeline.items()[0]
    assert item["output"] == os.path.join(
        pipeline.production.rundir, "results", "H1", "merged_J0534+2200_H1_2f.hdf5"
    )


def test_a_shard_that_is_not_complete_is_an_error_and_not_a_gap_in_the_data(project, provided):
    del provided["shard-0002"]
    with pytest.raises(PipelineException, match="shard-0002 has no heterodyned files"):
        make(project).items()


def test_a_shard_that_was_given_up_on_is_left_out(project, provided):
    pipeline = make(project)
    project.set_status("O4a-heterodyne", "shard-0002", "skipped")
    del provided["shard-0002"]
    pipeline = project.analysis("O4a-heterodyne", "merge").pipeline
    assert [len(i["inputs"]) for i in pipeline.items()] == [1, 1]


def test_there_is_nothing_to_merge_without_a_shard(project, provided):
    project.apply(
        "kind: event\nname: O4a-heterodyne\n---\nkind: analysis\nname: merge\n"
        "pipeline: cwinpy-merge\nevent: O4a-heterodyne\n"
    )
    pipeline = project.analysis("O4a-heterodyne", "merge").pipeline
    with pytest.raises(PipelineException, match="nothing to merge"):
        pipeline.items()


def test_an_analysis_that_is_not_a_shard_is_not_asked_for_files(project, provided):
    pipeline = make(project, other=True)
    assert "other" in [d.name for d in pipeline.production.dependency_objects]
    assert len(pipeline.items()) == 2


def test_it_needs_the_heterodyned_files(project, provided):
    assert make(project).get_actual_inputs(None) == [SHARD_ASSET]


def test_by_time_orders_by_the_start_of_the_data():
    names = ["x_1000002048-1000004096.hdf5", "x_1000000000-1000002048.hdf5"]
    assert sorted(names, key=by_time) == names[::-1]


# The configuration is the plan


def test_the_configuration_is_the_plan_as_json(project, provided):
    pipeline = make(project)
    plan = json.loads(pipeline.render_config())
    assert plan["status"] == os.path.join(pipeline.production.rundir, "merge-status.json")
    assert plan["items"] == pipeline.items()


def test_asimov_makes_the_configuration_file_from_the_template(project, provided, tmp_path):
    pipeline = make(project)
    target = tmp_path / "merge.ini"
    pipeline.production.make_config(str(target))
    assert json.loads(target.read_text()) == pipeline.plan()


def test_the_check_passes_when_every_shard_is_complete(project, provided):
    make(project).before_config()


def test_the_check_fails_when_a_shard_is_not(project, provided):
    del provided["shard-0001"]
    with pytest.raises(PipelineException, match="shard-0001"):
        make(project).before_config()


def test_the_check_captures_the_environment(project, provided):
    make(project).before_config()
    MergePipeline._capture_environment.assert_called_once()


# Building and submitting


def configured(project, tmp_path):
    pipeline = make(project)
    config = tmp_path / "merge.ini"
    config.write_text(pipeline.render_config())
    repository = MagicMock(directory=str(tmp_path))
    repository.find_prods.return_value = [config.name]
    pipeline.production.event.repository = repository
    pipeline.category = ""
    return pipeline


def test_building_puts_the_plan_where_the_job_reads_it_and_makes_the_directories(
    project, provided, tmp_path
):
    pipeline = configured(project, tmp_path)
    assert pipeline.build_dag() == pipeline.plan_file()
    assert json.load(open(pipeline.plan_file())) == pipeline.plan()
    assert os.path.isdir(os.path.dirname(pipeline.items()[0]["output"]))


def test_a_dry_run_builds_nothing(project, provided, tmp_path):
    pipeline = configured(project, tmp_path)
    pipeline.build_dag(dryrun=True)
    assert not os.path.exists(pipeline.plan_file())


def test_a_configuration_that_is_not_a_plan_is_an_error(project, provided, tmp_path):
    pipeline = configured(project, tmp_path)
    (tmp_path / "merge.ini").write_text("[not]\njson = true\n")
    with pytest.raises(PipelineException, match="not the plan of a merge"):
        pipeline.build_dag()


def test_building_without_a_configuration_is_an_error(project, provided, tmp_path):
    pipeline = configured(project, tmp_path)
    pipeline.production.event.repository.find_prods.return_value = []
    with pytest.raises(PipelineException, match="No configuration file found"):
        pipeline.build_dag()


@pytest.fixture
def submitting(project, provided, tmp_path):
    pipeline = configured(project, tmp_path)
    scheduler = MagicMock()
    scheduler.submit.return_value = 77
    with patch.object(
        MergePipeline, "scheduler", new_callable=PropertyMock, return_value=scheduler
    ):
        yield pipeline, scheduler


def test_submitting_runs_the_job_with_the_plan(submitting):
    pipeline, scheduler = submitting
    assert pipeline.submit_dag() == 77
    job = scheduler.submit.call_args[0][0]
    assert job.kwargs["arguments"] == f"-m asimov_cwinpy.merge_job {pipeline.plan_file()}"
    assert os.path.exists(job.executable)
    assert pipeline.production.status == "running"
    assert pipeline.production.job_id == 77


def test_the_job_asks_for_the_resources_in_the_ledger(project, provided, tmp_path):
    project.apply(blueprint(extra="scheduler:\n  request memory: 8192MB\n  request cpus: 2\n"))
    pipeline = configured_from(project, tmp_path)
    scheduler = MagicMock()
    scheduler.submit.return_value = 1
    with patch.object(
        MergePipeline, "scheduler", new_callable=PropertyMock, return_value=scheduler
    ):
        pipeline.submit_dag()
    job = scheduler.submit.call_args[0][0]
    assert str(job.kwargs["cpus"]) == "2" and str(job.kwargs["memory"]) == "8192MB"


def configured_from(project, tmp_path):
    pipeline = project.analysis("O4a-heterodyne", "merge").pipeline
    config = tmp_path / "merge.ini"
    config.write_text(pipeline.render_config())
    repository = MagicMock(directory=str(tmp_path))
    repository.find_prods.return_value = [config.name]
    pipeline.production.event.repository = repository
    pipeline.category = ""
    return pipeline


def test_what_the_last_run_said_is_not_what_this_one_will_say(submitting):
    pipeline, _ = submitting
    pipeline.build_dag()
    open(pipeline.status_file(), "w").write(json.dumps({"status": "failed", "error": "x"}))
    pipeline.submit_dag()
    assert pipeline.job_status() is None


def test_a_dry_run_submits_nothing(submitting):
    pipeline, scheduler = submitting
    assert pipeline.submit_dag(dryrun=True) is None
    scheduler.submit.assert_not_called()


def test_a_scheduler_that_fails_is_a_pipeline_error(submitting):
    pipeline, scheduler = submitting
    scheduler.submit.side_effect = RuntimeError("no schedd")
    with pytest.raises(PipelineException, match="could not be submitted: no schedd"):
        pipeline.submit_dag()


def test_the_python_is_the_one_the_ledger_names_if_it_exists(project, provided, tmp_path):
    pipeline = make(project)
    pipeline.production.meta["executable"] = sys.executable
    assert pipeline._python() == sys.executable
    pipeline.production.meta["executable"] = "/no/such/python"
    assert pipeline._python() == sys.executable  # falls back to the one asimov runs in


# Knowing how it went


@pytest.fixture
def planned(project, provided, tmp_path):
    pipeline = configured(project, tmp_path)
    pipeline.build_dag()
    return pipeline


def said(pipeline, **status):
    with open(pipeline.status_file(), "w") as handle:
        json.dump(status, handle)


def merged(pipeline, only=None):
    for item in pipeline.planned()[:only]:
        open(item["output"], "w").write("data")


def test_it_is_not_complete_before_it_is_planned(project, provided):
    assert not make(project).detect_completion()


def test_it_is_not_complete_before_the_job_has_said_so(planned):
    merged(planned)
    assert not planned.detect_completion()


def test_it_is_complete_when_the_job_succeeded_and_every_merged_file_has_data(planned):
    merged(planned)
    said(planned, status="succeeded", merged=2)
    assert planned.detect_completion()


def test_a_merged_file_that_is_missing_or_empty_is_not_complete(planned):
    merged(planned, only=1)
    said(planned, status="succeeded")
    assert not planned.detect_completion()
    open(planned.planned()[1]["output"], "w").close()
    assert planned.incomplete_outputs() == [planned.planned()[1]["output"]]
    assert not planned.detect_completion()


def test_a_job_that_failed_is_not_complete_though_the_files_are_there(planned):
    merged(planned)
    said(planned, status="failed", error="x")
    assert not planned.detect_completion()


def test_a_job_that_has_said_nothing_is_left_alone(planned):
    planned.resurrect()


def test_a_job_that_succeeded_is_left_alone(planned):
    merged(planned)
    said(planned, status="succeeded")
    planned.resurrect()


def test_a_job_that_failed_is_a_failure_with_the_reason(planned):
    said(planned, status="failed", error="ValueError: bad", transient=False)
    with pytest.raises(CwinpyError, match="failed: ValueError: bad") as caught:
        planned.resurrect()
    assert caught.value.transient is False


def test_a_job_that_failed_because_the_file_system_was_unwell_is_transient(planned):
    said(planned, status="failed", error="OSError: [Errno 5] Input/output error", transient=True)
    with pytest.raises(TransientCwinpyError) as caught:
        planned.resurrect()
    assert caught.value.transient is True


def test_a_job_that_succeeded_without_its_files_is_a_failure(planned):
    merged(planned, only=1)
    said(planned, status="succeeded")
    with pytest.raises(CwinpyError, match="1 of its merged files are missing or empty"):
        planned.resurrect()


# What it provides


def test_there_are_no_assets_until_it_is_complete(planned):
    assert planned.collect_assets() == {}


def test_the_assets_are_the_merged_files_by_pulsar_detector_and_factor(planned):
    merged(planned)
    said(planned, status="succeeded")
    data = planned.collect_assets()[ASSET]
    assert sorted(data["J0534+2200"]) == ["H1", "L1"]
    assert data["J0534+2200"]["H1"][2].endswith("merged_J0534+2200_H1_2f.hdf5")
    assert planned.output_paths()[ASSET] == [i["output"] for i in planned.planned()]
