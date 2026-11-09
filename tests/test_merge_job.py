"""The job that merges heterodyned files, without cwinpy."""

import errno
import json
import os
import subprocess
import sys
import types

import pytest

from asimov_cwinpy import merge_job


class FakeMerge:
    """What cwinpy's heterodyne_merge does that matters here."""

    def __init__(self, fail=None):
        self.calls = []
        self.fail = fail

    def __call__(self, heterodynedfiles, output, overwrite):
        self.calls.append({"inputs": heterodynedfiles, "output": output, "overwrite": overwrite})
        if self.fail:
            raise self.fail
        with open(output, "w") as handle:
            handle.write("merged")


@pytest.fixture
def merge(monkeypatch):
    fake = FakeMerge()
    module = types.ModuleType("cwinpy.heterodyne")
    module.heterodyne_merge = fake
    monkeypatch.setitem(sys.modules, "cwinpy", types.ModuleType("cwinpy"))
    monkeypatch.setitem(sys.modules, "cwinpy.heterodyne", module)
    return fake


def inputs(tmp_path, *names, content="data"):
    paths = []
    for name in names:
        path = tmp_path / name
        path.write_text(content)
        paths.append(str(path))
    return paths


def plan(tmp_path, *items):
    status = tmp_path / "status.json"
    path = tmp_path / "plan.json"
    path.write_text(json.dumps({"status": str(status), "items": list(items)}))
    return str(path), status


def item(tmp_path, inputs, pulsar="J0534+2200", detector="H1", factor=2):
    return {
        "pulsar": pulsar,
        "detector": detector,
        "factor": factor,
        "inputs": inputs,
        "output": str(tmp_path / "results" / detector / f"merged_{pulsar}_{factor}.hdf5"),
    }


def said(status):
    return json.loads(status.read_text())


def test_every_merge_in_the_plan_is_made_and_the_status_says_so(merge, tmp_path):
    first = item(tmp_path, inputs(tmp_path, "a1", "a2"))
    second = item(tmp_path, inputs(tmp_path, "b1"), detector="L1")
    path, status = plan(tmp_path, first, second)
    assert merge_job.run(path) == 0
    assert [call["output"] for call in merge.calls] == [first["output"], second["output"]]
    assert merge.calls[0]["inputs"] == first["inputs"]
    assert said(status) == {"status": "succeeded", "merged": 2}
    assert os.path.exists(first["output"]) and os.path.exists(second["output"])


def test_an_existing_merged_file_is_replaced_because_the_last_one_may_be_half_written(
    merge, tmp_path
):
    path, _ = plan(tmp_path, item(tmp_path, inputs(tmp_path, "a")))
    merge_job.run(path)
    assert merge.calls[0]["overwrite"] is True


def test_the_status_is_written_all_at_once(merge, tmp_path):
    path, status = plan(tmp_path, item(tmp_path, inputs(tmp_path, "a")))
    merge_job.run(path)
    assert not os.path.exists(f"{status}.tmp")


def test_an_input_that_is_missing_stops_everything_and_is_transient(merge, tmp_path):
    good = item(tmp_path, inputs(tmp_path, "a"))
    bad = item(tmp_path, [str(tmp_path / "gone.hdf5"), *inputs(tmp_path, "c")], detector="L1")
    path, status = plan(tmp_path, good, bad)
    assert merge_job.run(path) == 1
    assert merge.calls == []  # not even the good one: cwinpy would have dropped the missing file
    said_ = said(status)
    assert said_["status"] == "failed" and said_["transient"] is True
    assert "1 input file(s)" in said_["error"] and "gone.hdf5" in said_["error"]


def test_an_input_that_is_empty_stops_everything_and_is_not_transient(merge, tmp_path):
    empty = inputs(tmp_path, "empty", content="")
    path, status = plan(tmp_path, item(tmp_path, [*empty, *inputs(tmp_path, "c")]))
    assert merge_job.run(path) == 1
    assert merge.calls == []
    said_ = said(status)
    assert said_["transient"] is False and "empty" in said_["error"]


def test_a_failure_of_cwinpy_is_reported_and_not_transient(merge, tmp_path):
    merge.fail = ValueError("None of the heterodyned files given exists!")
    path, status = plan(tmp_path, item(tmp_path, inputs(tmp_path, "a")))
    assert merge_job.run(path) == 1
    said_ = said(status)
    assert said_["status"] == "failed" and said_["transient"] is False
    assert said_["error"] == "ValueError: None of the heterodyned files given exists!"


@pytest.mark.parametrize("number", [errno.EIO, errno.ESTALE, errno.ETIMEDOUT])
def test_a_file_system_that_is_unwell_is_transient(merge, tmp_path, number):
    merge.fail = OSError(number, "unwell")
    path, status = plan(tmp_path, item(tmp_path, inputs(tmp_path, "a")))
    merge_job.run(path)
    assert said(status)["transient"] is True


def test_another_os_error_is_not_transient(merge, tmp_path):
    merge.fail = PermissionError(errno.EACCES, "no")
    path, status = plan(tmp_path, item(tmp_path, inputs(tmp_path, "a")))
    merge_job.run(path)
    assert said(status)["transient"] is False


def test_a_failure_leaves_its_traceback_in_the_error_output(merge, tmp_path, capsys):
    merge.fail = ValueError("boom")
    path, _ = plan(tmp_path, item(tmp_path, inputs(tmp_path, "a")))
    merge_job.run(path)
    assert "Traceback (most recent call last)" in capsys.readouterr().err


def test_cwinpy_missing_from_the_environment_of_the_job_is_a_failure(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "cwinpy.heterodyne", None)
    path, status = plan(tmp_path, item(tmp_path, inputs(tmp_path, "a")))
    assert merge_job.run(path) == 1
    assert said(status)["error"].startswith("ModuleNotFoundError")


def test_a_plan_with_nothing_to_merge_succeeds(merge, tmp_path):
    path, status = plan(tmp_path)
    assert merge_job.run(path) == 0
    assert said(status)["merged"] == 0


def test_main_needs_a_plan():
    with pytest.raises(SystemExit, match="usage"):
        merge_job.main([])


def test_it_runs_as_a_module_and_says_how_it_went(tmp_path):
    """As the scheduler runs it: ``python -m asimov_cwinpy.merge_job plan.json``."""
    path, status = plan(tmp_path, item(tmp_path, [str(tmp_path / "gone.hdf5")]))
    done = subprocess.run(
        [sys.executable, "-m", "asimov_cwinpy.merge_job", path], capture_output=True, text=True
    )
    assert done.returncode == 1
    assert said(status)["status"] == "failed"
    assert "gone.hdf5" in done.stderr
