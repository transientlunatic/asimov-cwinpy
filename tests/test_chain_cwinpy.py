"""
The whole chain with the real cwinpy: two shards of a run are heterodyned, the merge joins
them, and the parameters of the pulsar are estimated from the merged data.

Every step is done in the way that asimov does it: the shards by the minimal DAGMan of
``tests/minidag.py``, then ``asimov manage build``, ``manage submit`` and ``monitor`` for the
rest, with the local scheduler. What is checked is that what one pipeline provides is what
the next one reads.

Skipped unless cwinpy is installed.
"""

import configparser
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from . import minidag

BIN = Path(sys.executable).parent

pytestmark = pytest.mark.skipif(
    not (BIN / "cwinpy_heterodyne").exists(), reason="cwinpy is not installed"
)

SHARDS = {"shard-0001": (1000000000, 1000002048), "shard-0002": (1000002048, 1000004096)}


@pytest.fixture(scope="module")
def frames(tmp_path_factory):
    pytest.importorskip("gwpy")
    from .frames import PAR, make_frames

    root = tmp_path_factory.mktemp("frames")
    made = make_frames(root, count=4)
    (root / "J0534+2200.par").write_text(PAR)
    for detector in made["caches"]:
        (root / f"segments_{detector}.txt").write_text(f"1000000000 {made['end']}\n")
    return {"root": root, "caches": made["caches"]}


def blueprint(frames):
    root = frames["root"]
    caches = frames["caches"]
    text = "kind: event\nname: O4a-heterodyne\ninterferometers: [H1, L1]\n"
    for name, (start, end) in SHARDS.items():
        text += (
            f"---\nkind: analysis\nname: {name}\npipeline: cwinpy-heterodyne\n"
            "event: O4a-heterodyne\ndata:\n"
            "  channels: {H1: 'H1:FAKE-STRAIN', L1: 'L1:FAKE-STRAIN'}\n"
            f"  cache files: {{H1: {caches['H1']}, L1: {caches['L1']}}}\n"
            f"cwinpy-heterodyne:\n  data start: {start}\n  data end: {end}\n"
            f"  ephemerides: [{root / 'J0534+2200.par'}]\n"
            f"  segment lists: {{H1: {root / 'segments_H1.txt'}, L1: {root / 'segments_L1.txt'}}}\n"
        )
    text += (
        "---\nkind: analysis\nname: merge\npipeline: cwinpy-merge\nevent: O4a-heterodyne\n"
        "needs: [shard-0001, shard-0002]\n"
        f"---\nkind: event\nname: J0534+2200\nephemeris: {root / 'J0534+2200.par'}\n"
        "interferometers: [H1, L1]\n"
        "priors:\n  h0: {type: uniform, minimum: 0.0, maximum: 1.0e-22}\n"
        "  phi0: {type: uniform, minimum: 0.0, maximum: 3.141592653589793}\n"
        "  iota: {type: sine, minimum: 0.0, maximum: 3.141592653589793}\n"
        "  psi: {type: uniform, minimum: 0.0, maximum: 1.5707963267948966}\n"
        "---\nkind: analysis\nname: pe\npipeline: cwinpy-pe\nevent: J0534+2200\n"
        "needs: ['O4a-heterodyne/merge']\n"
        "sampler:\n  sampler: dynesty\n  sampler kwargs: {nlive: 100, dlogz: 1.0}\n"
    )
    return text


def strategy_blueprint(frames):
    """The same run, as the one blueprint that the ``cwinpy-run`` strategy expands."""
    root = frames["root"]
    caches = frames["caches"]
    (start, _), (_, end) = SHARDS["shard-0001"], SHARDS["shard-0002"]
    return (
        "kind: analysis\nname: O4a\nstrategy:\n  type: cwinpy-run\n"
        f"  run: O4a-heterodyne\n  interferometers: [H1, L1]\n  data start: {start}\n"
        f"  data end: {end}\n  joblength: 2048\n  shards: {{count: 2}}\n"
        f"  pulsars: [{root / 'J0534+2200.par'}]\n  heterodyne:\n    data:\n"
        "      channels: {H1: 'H1:FAKE-STRAIN', L1: 'L1:FAKE-STRAIN'}\n"
        f"      cache files: {{H1: {caches['H1']}, L1: {caches['L1']}}}\n"
        f"    settings:\n      segment lists: {{H1: {root / 'segments_H1.txt'}, "
        f"L1: {root / 'segments_L1.txt'}}}\n"
        "  pe:\n    priors:\n      h0: {type: uniform, minimum: 0.0, maximum: 1.0e-22}\n"
        "      phi0: {type: uniform, minimum: 0.0, maximum: 3.141592653589793}\n"
        "      iota: {type: sine, minimum: 0.0, maximum: 3.141592653589793}\n"
        "      psi: {type: uniform, minimum: 0.0, maximum: 1.5707963267948966}\n"
        "    sampler: {sampler: dynesty, sampler kwargs: {nlive: 100, dlogz: 1.0}}\n"
    )


def asimov(project, *arguments):
    env = dict(os.environ, PATH=f"{BIN}{os.pathsep}{os.environ['PATH']}")
    done = subprocess.run(
        [str(BIN / "asimov"), *arguments],
        cwd=project.root,
        env=env,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        timeout=300,
    )
    assert done.returncode == 0, f"asimov {' '.join(arguments)}:\n{done.stdout}{done.stderr}"
    return done.stdout


def wait_for(project, subject, name, seconds=240):
    """Run the monitor until an analysis is finished."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        time.sleep(4)
        asimov(project, "monitor")
        if project.analysis(subject, name).status == "finished":
            return
    pytest.fail(f"{subject}/{name} is {project.analysis(subject, name).status}, not finished")


@pytest.mark.parametrize("source", ["explicit", "strategy"])
def test_two_shards_are_merged_and_the_merge_is_analysed(
    database_project, monkeypatch, frames, tmp_path, source
):
    project = database_project
    # The project runs jobs itself, with the cwinpy of this environment.
    conf = Path(".asimov/asimov.conf")
    settings = configparser.ConfigParser()
    settings.read(conf)
    settings["scheduler"] = {"type": "local"}
    settings["pipelines"] = {"environment": sys.prefix}
    with open(conf, "w") as handle:
        settings.write(handle)
    monkeypatch.setenv("PATH", f"{BIN}{os.pathsep}{os.environ['PATH']}")

    project.apply((blueprint if source == "explicit" else strategy_blueprint)(frames))
    asimov(project, "manage", "build")  # builds the shards: nothing else is ready

    # The shards: DAGs, which HTCondor would run
    for name in SHARDS:
        shard = project.analysis("O4a-heterodyne", name)
        pipeline = shard.pipeline
        pipeline.build_dag()
        codes = minidag.run_dag(pipeline.dag_file(), str(tmp_path / name))
        assert set(codes.values()) == {0}, codes
        with open(pipeline.dag_file() + ".dagman.out", "w") as handle:
            handle.write("EXITING WITH STATUS 0\n")
        # as if submitted: the monitor finds that it is done
        project.set_status("O4a-heterodyne", name, "running")
    asimov(project, "monitor")
    for name in SHARDS:
        shard = project.analysis("O4a-heterodyne", name)
        assert shard.status == "finished"
        # What cwinpy heterodyned of the science time is measured from its own files: a job
        # of 2048 s loses the start and end of its data (about 90 s at each end)
        for detector, found in shard.meta["coverage"].items():
            assert found["science"] == 2048, (detector, found)
            assert 0.85 < found["fraction"] < 0.95, (detector, found)

    # The merge: now that its shards are finished it can be built and run
    asimov(project, "manage", "build")
    asimov(project, "manage", "submit")
    wait_for(project, "O4a-heterodyne", "merge")
    merge = project.analysis("O4a-heterodyne", "merge").pipeline
    merged = merge.collect_assets()["heterodyned data"]["J0534+2200"]
    assert sorted(merged) == ["H1", "L1"]
    from cwinpy.data import HeterodynedData

    data = HeterodynedData.read(merged["H1"][2])
    assert data.times[0].value >= SHARDS["shard-0001"][0]
    assert data.times[-1].value > SHARDS["shard-0002"][0]  # the second shard is in it

    # The parameter estimation reads the merge
    asimov(project, "manage", "build")
    asimov(project, "manage", "submit")
    wait_for(project, "J0534+2200", "pe")
    result = project.analysis("J0534+2200", "pe").pipeline.collect_assets()["samples"]
    import bilby

    posterior = bilby.result.read_in_result(result)
    assert sorted(posterior.search_parameter_keys) == ["h0", "iota", "phi0", "psi"]
    assert len(posterior.posterior) > 10

    # Everything is finished: the per-chunk files, which the merge has used, may be released,
    # and what the parameter estimation reads and made is not
    merged_files = [path for by_factor in merged.values() for path in by_factor.values()]
    chunks = [
        path
        for name in SHARDS
        for path in project.analysis("O4a-heterodyne", name).pipeline.expected_files()
    ]
    assert all(os.path.getsize(path) > 0 for path in chunks)
    output = asimov(project, "analysis", "prune", "--yes")
    assert "Released" in output, output
    assert not any(os.path.exists(path) for path in chunks)
    assert all(os.path.getsize(path) > 0 for path in merged_files)
    assert os.path.exists(result)
    asimov(project, "monitor")  # nothing is disturbed by the files being gone
    for name in SHARDS:
        assert project.analysis("O4a-heterodyne", name).status == "finished"
    assert project.analysis("O4a-heterodyne", "merge").status == "finished"
    assert project.analysis("J0534+2200", "pe").status == "finished"
