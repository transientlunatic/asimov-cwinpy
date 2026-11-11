"""The job that makes the summary pages, without cwinpy."""

import errno
import json
import os
import subprocess
import sys
import types

import pytest

from asimov_cwinpy import summary_job


class FakeGenerator:
    """What cwinpy's generate_summary_pages does that matters here."""

    def __init__(self, fail=None, make_home=True):
        self.calls = []
        self.fail = fail
        self.make_home = make_home

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise self.fail
        if self.make_home:
            home = os.path.join(kwargs["outpath"], "html", "home.html")
            os.makedirs(os.path.dirname(home), exist_ok=True)
            with open(home, "w") as handle:
                handle.write("<html>")


@pytest.fixture
def generator(monkeypatch):
    # (whether pkg_resources is installed is not what these tests are about)
    monkeypatch.setitem(sys.modules, "pkg_resources", types.ModuleType("pkg_resources"))
    fake = FakeGenerator()
    module = types.ModuleType("cwinpy.pe.summary")
    module.generate_summary_pages = fake
    for name in ("cwinpy", "cwinpy.pe"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, "cwinpy.pe.summary", module)
    return fake


def plan(tmp_path, inputs=(), options=None):
    status = tmp_path / "status.json"
    path = tmp_path / "plan.json"
    path.write_text(
        json.dumps(
            {
                "status": str(status),
                "config": str(tmp_path / "summary.ini"),
                "outpath": str(tmp_path / "pages"),
                "url": "O4a/summary",
                "options": options or {},
                "inputs": list(inputs),
            }
        )
    )
    return str(path), status


def present(tmp_path, *names, content="data"):
    paths = []
    for name in names:
        path = tmp_path / name
        path.write_text(content)
        paths.append(str(path))
    return paths


def said(status):
    return json.loads(status.read_text())


def test_the_pages_are_made_with_what_the_plan_says_and_the_status_says_so(generator, tmp_path):
    path, status = plan(tmp_path, present(tmp_path, "a", "b"), {"showtimeseries": False})
    assert summary_job.run(path) == 0
    assert generator.calls == [
        {
            "config": str(tmp_path / "summary.ini"),
            "outpath": str(tmp_path / "pages"),
            "url": "O4a/summary",
            "showtimeseries": False,
        }
    ]
    assert said(status) == {
        "status": "succeeded",
        "home": str(tmp_path / "pages" / "html" / "home.html"),
    }


def test_an_input_that_is_missing_stops_it_before_cwinpy_and_is_transient(generator, tmp_path):
    path, status = plan(tmp_path, present(tmp_path, "a") + [str(tmp_path / "gone")])
    assert summary_job.run(path) == 1
    assert generator.calls == []
    found = said(status)
    assert found["status"] == "failed" and found["transient"] is True
    assert "1 input file(s)" in found["error"] and "gone" in found["error"]


def test_an_input_that_is_empty_is_not_transient(generator, tmp_path):
    path, status = plan(tmp_path, present(tmp_path, "a", content=""))
    assert summary_job.run(path) == 1
    assert generator.calls == []
    assert said(status)["transient"] is False
    assert "empty" in said(status)["error"]


def test_a_failure_of_cwinpy_is_reported_and_not_transient(generator, tmp_path):
    generator.fail = ValueError("The time bin selected is invalid")
    path, status = plan(tmp_path, present(tmp_path, "a"))
    assert summary_job.run(path) == 1
    found = said(status)
    assert found["status"] == "failed" and found["transient"] is False
    assert found["error"] == "ValueError: The time bin selected is invalid"


@pytest.mark.parametrize("number", [errno.EIO, errno.ESTALE, errno.ETIMEDOUT])
def test_a_file_system_that_is_unwell_is_transient(generator, tmp_path, number):
    generator.fail = OSError(number, "unwell")
    path, status = plan(tmp_path, present(tmp_path, "a"))
    summary_job.run(path)
    assert said(status)["transient"] is True


def test_a_missing_pkg_resources_is_said_in_words_that_say_what_to_install(
    generator, tmp_path, monkeypatch
):
    monkeypatch.setitem(sys.modules, "pkg_resources", None)
    path, status = plan(tmp_path, present(tmp_path, "a"))
    assert summary_job.run(path) == 1
    found = said(status)
    assert found["transient"] is False
    assert "setuptools<82" in found["error"] and "pkg_resources" in found["error"]
    assert generator.calls == []  # before cwinpy is run


def test_pkg_resources_being_there_is_not_a_problem(generator, tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "pkg_resources", types.ModuleType("pkg_resources"))
    path, status = plan(tmp_path, present(tmp_path, "a"))
    assert summary_job.run(path) == 0


def test_pages_that_cwinpy_did_not_make_are_a_failure(generator, tmp_path):
    generator.make_home = False
    path, status = plan(tmp_path, present(tmp_path, "a"))
    assert summary_job.run(path) == 1
    assert "did not make" in said(status)["error"]


def test_a_failure_leaves_its_traceback_in_the_error_output(generator, tmp_path, capsys):
    generator.fail = ValueError("boom")
    path, _ = plan(tmp_path, present(tmp_path, "a"))
    summary_job.run(path)
    assert "Traceback (most recent call last)" in capsys.readouterr().err


def test_cwinpy_missing_from_the_environment_of_the_job_is_a_failure(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "cwinpy.pe.summary", None)
    path, status = plan(tmp_path, present(tmp_path, "a"))
    assert summary_job.run(path) == 1
    assert said(status)["error"].startswith("ModuleNotFoundError")


def test_main_needs_a_plan():
    with pytest.raises(SystemExit, match="usage"):
        summary_job.main([])


def test_it_runs_as_a_module_and_says_how_it_went(tmp_path):
    """As the scheduler runs it: ``python -m asimov_cwinpy.summary_job plan.json``."""
    path, status = plan(tmp_path, [str(tmp_path / "gone.hdf5")])
    done = subprocess.run(
        [sys.executable, "-m", "asimov_cwinpy.summary_job", path], capture_output=True, text=True
    )
    assert done.returncode == 1
    assert said(status)["status"] == "failed"
    assert "gone.hdf5" in done.stderr
