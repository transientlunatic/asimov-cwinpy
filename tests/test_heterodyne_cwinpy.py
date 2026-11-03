"""
The cwinpy-heterodyne pipeline with the real cwinpy: its DAG runner builds the DAG, and the
real ``cwinpy_heterodyne`` runs the jobs, on fake frames.

Skipped unless cwinpy is installed. HTCondor is not needed: ``tests/minidag.py`` runs the
jobs of the DAG, and the tests say what DAGMan would have said.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from asimov_cwinpy.common import CwinpyError, TransientCwinpyError
from asimov_cwinpy.heterodyne import ASSET

from . import minidag

BIN = Path(sys.executable).parent

pytestmark = pytest.mark.skipif(
    not (BIN / "cwinpy_heterodyne").exists(), reason="cwinpy is not installed"
)


@pytest.fixture(scope="module")
def frames(tmp_path_factory):
    """Fake frames, a parameter file and segment lists, made once."""
    pytest.importorskip("gwpy")
    from .frames import PAR, make_frames

    root = tmp_path_factory.mktemp("frames")
    made = make_frames(root)
    (root / "J0534+2200.par").write_text(PAR)
    for detector in made["caches"]:
        (root / f"segments_{detector}.txt").write_text(f"1000000000 {made['end']}\n")
    return {"root": root, "end": made["end"], "caches": made["caches"]}


def blueprint(frames, caches, extra_event="", extra_analysis=""):
    root = frames["root"]
    return (
        f"kind: event\nname: O4a-heterodyne\ninterferometers: [H1, L1]\n{extra_event}---\n"
        "kind: analysis\nname: shard-0001\npipeline: cwinpy-heterodyne\nevent: O4a-heterodyne\n"
        "data:\n  channels: {H1: 'H1:FAKE-STRAIN', L1: 'L1:FAKE-STRAIN'}\n"
        f"  cache files: {{H1: {caches['H1']}, L1: {caches['L1']}}}\n"
        f"cwinpy-heterodyne:\n  data start: 1000000000\n  data end: {frames['end']}\n"
        f"  ephemerides: [{root / 'J0534+2200.par'}]\n"
        f"  segment lists: {{H1: {root / 'segments_H1.txt'}, L1: {root / 'segments_L1.txt'}}}\n"
        f"{extra_analysis}"
    )


def shard(project, monkeypatch, text):
    """Apply the blueprint, have asimov make the configuration, and build the DAG."""
    monkeypatch.setenv("PATH", f"{BIN}{os.pathsep}{os.environ['PATH']}")
    project.apply(text)
    built = subprocess.run(
        [str(BIN / "asimov"), "manage", "build"],
        cwd=project.root,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        timeout=300,
    )
    assert built.returncode == 0, built.stdout + built.stderr
    pipeline = project.analysis("O4a-heterodyne", "shard-0001").pipeline
    pipeline.build_dag()
    return pipeline


def dagman_says(pipeline, status):
    with open(pipeline.dag_file() + ".dagman.out", "w") as handle:
        handle.write(f"EXITING WITH STATUS {status}\n")


def damaged(frames, tmp_path):
    """Frames with one of H1's cut short, as if its disk had gone away part way through."""
    caches = {}
    for detector, directory in frames["caches"].items():
        copy = tmp_path / "damaged" / detector
        shutil.copytree(directory, copy)
        caches[detector] = str(copy)
    victim = sorted((tmp_path / "damaged" / "H1").glob("*.gwf"))[-1]
    victim.write_bytes(victim.read_bytes()[:30000])
    return caches


def test_cwinpys_runner_makes_the_dag_and_an_empty_file_for_each_output(
    project, monkeypatch, frames
):
    pipeline = shard(
        project,
        monkeypatch,
        blueprint(frames, frames["caches"], extra_event="frequency factors: [1, 2]\n"),
    )
    nodes = minidag.read_dag(pipeline.dag_file())
    assert len(nodes) == 4  # two detectors, two factors
    assert len(pipeline.expected_files()) == 4
    assert all(f.startswith(pipeline.production.rundir) for f in pipeline.expected_files())
    # cwinpy makes an empty file for each output, which is why existing is not complete
    assert all(os.path.getsize(f) == 0 for f in pipeline.expected_files())
    assert not pipeline.detect_completion()


def test_the_jobs_make_data_and_the_shard_is_complete_when_dagman_has_succeeded(
    project, monkeypatch, frames, tmp_path
):
    pipeline = shard(
        project,
        monkeypatch,
        blueprint(frames, frames["caches"], extra_event="frequency factors: [1, 2]\n"),
    )
    codes = minidag.run_dag(pipeline.dag_file(), str(tmp_path / "scratch"))
    assert set(codes.values()) == {0}, codes
    assert pipeline.incomplete_outputs() == []
    assert not pipeline.detect_completion()  # DAGMan has not said so
    dagman_says(pipeline, 0)
    assert pipeline.detect_completion()

    files = pipeline.collect_assets()[ASSET]["J0534+2200"]
    assert sorted(files) == ["H1", "L1"] and sorted(files["H1"]) == [1, 2]
    assert all(os.path.getsize(f) > 0 for f in files["H1"][2])


def test_a_frame_that_cannot_be_read_fails_the_job_and_is_a_transient_failure(
    project, monkeypatch, frames, tmp_path
):
    pipeline = shard(project, monkeypatch, blueprint(frames, damaged(frames, tmp_path)))
    codes = minidag.run_dag(pipeline.dag_file(), str(tmp_path / "scratch"))
    failed = sorted(name for name, code in codes.items() if code != 0)
    assert len(failed) == 1 and "H1" in failed[0], codes

    assert not pipeline.detect_completion()
    assert len(pipeline.incomplete_outputs()) == 1
    dagman_says(pipeline, 1)  # DAGMan: one node failed, and a rescue file was written
    with pytest.raises(TransientCwinpyError, match="1 job.*could not read frame files") as caught:
        pipeline.resurrect()
    assert caught.value.transient is True
    assert "H1" in str(caught.value)


def test_a_frame_that_cannot_be_read_loses_the_data_without_a_failure_unless_the_job_is_strict(
    project, monkeypatch, frames, tmp_path
):
    """cwinpy's own default: the job succeeds, and has written nothing."""
    pipeline = shard(
        project,
        monkeypatch,
        blueprint(
            frames,
            damaged(frames, tmp_path),
            extra_analysis="  strict data requirement: false\n",
        ),
    )
    codes = minidag.run_dag(pipeline.dag_file(), str(tmp_path / "scratch"))
    assert set(codes.values()) == {0}, codes  # no job failed
    dagman_says(pipeline, 0)  # and DAGMan succeeded
    assert pipeline.dag_outcome() == "succeeded"

    assert not pipeline.detect_completion()  # but there is no data from H1
    with pytest.raises(CwinpyError, match="1 of its outputs are missing or empty") as caught:
        pipeline.resurrect()
    assert caught.value.transient is False
