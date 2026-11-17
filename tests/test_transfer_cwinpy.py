"""
The cwinpy-pe pipeline on a cluster whose execute nodes do not see the project: the real
``cwinpy_pe`` runs in a directory of its own, with the files that the plugin lists copied there and
nothing inherited from the environment of asimov, and its results are copied back.

``tests/minitransfer.py`` does what HTCondor does for such a job (there is no pool here), and hides
the whole project while the job runs, so that a path that the job reads from the project fails as it
would on a node. Skipped unless cwinpy is installed. It analyses simulated noise.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from asimov_cwinpy.pe import PEPipeline

from . import minitransfer

BIN = Path(sys.executable).parent

pytestmark = pytest.mark.skipif(not (BIN / "cwinpy_pe").exists(), reason="cwinpy is not installed")

PAR = "PSRJ J0534+2200\nRAJ 05:34:31.97232\nDECJ 22:00:52.0690\nF0 29.6\nPEPOCH 55555\n"

BLUEPRINT = """\
kind: event
name: J0534+2200
ephemeris: {par}
interferometers: [H1, L1]
priors:
  h0: {{type: uniform, minimum: 0.0, maximum: 1.0e-22}}
  phi0: {{type: uniform, minimum: 0.0, maximum: 3.141592653589793}}
  iota: {{type: sine, minimum: 0.0, maximum: 3.141592653589793}}
  psi: {{type: uniform, minimum: 0.0, maximum: 1.5707963267948966}}
---
kind: analysis
name: pe
pipeline: cwinpy-pe
event: J0534+2200
sampler:
  sampler: dynesty
  sampler kwargs: {{nlive: {nlive}, dlogz: 1.0}}
scheduler:
  transfer files: true
  environment: {environment}
{scheduler}cwinpy-pe:
  fake asd: {{H1: 1.0e-24, L1: 1.0e-24}}
  fake start: 1000000000
  fake end: 1000086400
  fake dt: 60
  fake seed: 1
"""


@pytest.fixture
def job(project, tmp_path):
    """The pipeline of a pulsar, ready to be submitted, and what watches the scheduler."""

    def make(scheduler="", nlive=100):
        par = tmp_path / "ephemerides" / "J0534+2200.par"
        par.parent.mkdir()
        par.write_text(PAR)
        project.apply(
            BLUEPRINT.format(par=par, environment=sys.prefix, scheduler=scheduler, nlive=nlive)
        )
        pipeline = project.analysis("J0534+2200", "pe").pipeline
        # what asimov does when it builds: it makes the configuration from the template
        config = tmp_path / "repo" / "pe.ini"
        config.parent.mkdir()
        with patch.object(PEPipeline, "_capture_environment"):
            pipeline.production.make_config(str(config))
        pipeline.production.event.repository = MagicMock(directory=str(tmp_path / "repo"))
        pipeline.production.event.repository.find_prods.return_value = ["pe.ini"]
        pipeline.category = ""
        scheduler = MagicMock()
        scheduler.submit.side_effect = [1, 2, 3]
        return pipeline, scheduler

    return make


def submit(pipeline, scheduler):
    with patch.object(PEPipeline, "scheduler", new_callable=PropertyMock, return_value=scheduler):
        pipeline.submit_dag()
    return scheduler.submit.call_args[0][0]


def test_the_job_runs_from_the_files_that_are_transferred_and_its_results_come_back(job, tmp_path):
    pipeline, scheduler = job()
    description = submit(pipeline, scheduler)
    ran = minitransfer.run_job(description, hide=[str(tmp_path)])
    assert ran.returncode == 0, ran.stderr[-3000:]
    # what the node had: the configuration, the prior, the ephemeris, and the (empty) results
    assert ran.scratch == sorted(["pe.ini", "pe.prior", "J0534+2200.par", "results", "cwinpy_pe"])
    assert ran.held == []
    # and what came back is where asimov looks for it
    assert pipeline.detect_completion()
    result = pipeline.collect_assets()["samples"]
    assert result == os.path.join(pipeline.production.rundir, "results", "pe_result.hdf5")
    import bilby

    posterior = bilby.result.read_in_result(result)
    assert sorted(posterior.search_parameter_keys) == ["h0", "iota", "phi0", "psi"]
    assert len(posterior.posterior) > 10


def test_the_output_and_error_of_the_job_come_back_to_the_run_directory(job, tmp_path):
    pipeline, scheduler = job()
    ran = minitransfer.run_job(submit(pipeline, scheduler), hide=[str(tmp_path)])
    assert ran.returncode == 0
    assert os.path.getsize(os.path.join(pipeline.production.rundir, "pe.err")) > 0


def test_the_job_does_not_need_anything_that_is_inherited_from_the_environment_of_asimov(
    job, tmp_path
):
    """The job runs with a path of /usr/bin:/bin and nothing else, and that is enough."""
    pipeline, scheduler = job()
    description = submit(pipeline, scheduler)
    assert "getenv" not in description.to_htcondor()
    assert minitransfer.run_job(description, hide=[str(tmp_path)]).returncode == 0


def test_a_job_stopped_to_be_restarted_gets_its_checkpoint_back_and_finishes(job, tmp_path):
    # (cwinpy_pe stops itself, after `periodic restart time`, with the exit code 77 that asimov
    # takes to mean that it is to be started again. The sampling is made long (500 live points,
    # well over four seconds) so that the alarm is always in the middle of it: with the short run
    # of the other tests a quick machine sometimes finished first, and the alarm then killed the
    # job in its post-processing instead of stopping it to be restarted.)
    pipeline, scheduler = job(scheduler="  periodic restart time: 4\n", nlive=500)
    first = minitransfer.run_job(submit(pipeline, scheduler), hide=[str(tmp_path)])
    assert first.returncode == 77, first.stderr[-3000:]
    results = os.path.join(pipeline.production.rundir, "results")
    checkpoint = [name for name in os.listdir(results) if "resume" in name]
    assert checkpoint, os.listdir(results)  # the checkpoint came back
    assert not pipeline.detect_completion()

    # asimov sees that it stopped to be restarted, and submits it again
    with patch.object(PEPipeline, "scheduler", new_callable=PropertyMock, return_value=scheduler):
        pipeline.resurrect()
    assert scheduler.submit.call_count == 2
    sent = scheduler.submit.call_args[0][0].to_htcondor()["transfer_input_files"]
    assert results in sent.split(", ")  # with the checkpoint in it
    # (a timer of seconds is only to make the first job stop: its alarm would go off again while
    # the second one starts, which one of hours would not)
    config = tmp_path / "repo" / "pe.ini"
    config.write_text(
        config.read_text().replace("periodic-restart-time = 4", "periodic-restart-time = 3600")
    )
    second = minitransfer.run_job(scheduler.submit.call_args[0][0], hide=[str(tmp_path)], cluster=2)
    assert second.returncode == 0, second.stderr[-3000:]
    assert "Resume file successfully loaded" in second.stderr  # it went on from the checkpoint
    assert pipeline.detect_completion()


def test_a_job_that_listed_a_file_it_did_not_make_is_held_by_htcondor_and_the_emulator_says_so(
    job, tmp_path
):
    pipeline, scheduler = job()
    description = submit(pipeline, scheduler)
    description.kwargs["transfer_output_files"] = "results, not_made"
    ran = minitransfer.run_job(description, hide=[str(tmp_path)])
    assert ran.held == ["not_made"]
