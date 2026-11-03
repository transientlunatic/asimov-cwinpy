"""
The cwinpy-pe pipeline running the real ``cwinpy_pe`` through asimov's command line.

Skipped unless cwinpy is installed (it is a large install, which the plugin does not
depend on). It analyses simulated noise, so it needs no data or cluster.
"""

import configparser
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

BIN = Path(sys.executable).parent
CWINPY_PE = BIN / "cwinpy_pe"

pytestmark = pytest.mark.skipif(not CWINPY_PE.exists(), reason="cwinpy is not installed")

PAR = """\
PSRJ     J0534+2200
RAJ      05:34:31.97232
DECJ     22:00:52.0690
F0       29.6
PEPOCH   55555
"""

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
  sampler kwargs: {{nlive: 100, dlogz: 1.0}}
cwinpy-pe:
  fake asd: {{H1: 1.0e-24, L1: 1.0e-24}}
  fake start: 1000000000
  fake end: 1000086400
  fake dt: 60
  fake seed: 1
"""


def asimov(*arguments, cwd):
    """Run the asimov command line, as the project's user would."""
    env = dict(os.environ, PATH=f"{BIN}{os.pathsep}{os.environ['PATH']}")
    return subprocess.run(
        [str(BIN / "asimov"), *arguments],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        timeout=300,
    )


def test_a_pulsar_is_analysed_from_blueprint_to_posterior(project, tmp_path):
    # The project runs jobs itself, with the cwinpy_pe of this environment.
    conf = Path(".asimov/asimov.conf")
    settings = configparser.ConfigParser()
    settings.read(conf)
    settings["scheduler"] = {"type": "local"}
    settings["pipelines"] = {"environment": sys.prefix}
    with open(conf, "w") as handle:
        settings.write(handle)

    par = tmp_path / "J0534+2200.par"
    par.write_text(PAR)
    project.apply(BLUEPRINT.format(par=par))

    built = asimov("manage", "build", cwd=tmp_path)
    assert built.returncode == 0, built.stdout + built.stderr
    submitted = asimov("manage", "submit", cwd=tmp_path)
    assert submitted.returncode == 0, submitted.stdout + submitted.stderr

    deadline = time.time() + 240
    while time.time() < deadline:
        time.sleep(5)
        asimov("monitor", cwd=tmp_path)
        analysis = project.analysis("J0534+2200", "pe")
        if analysis.status == "finished":
            break
    analysis = project.analysis("J0534+2200", "pe")
    assert analysis.status == "finished", analysis.status

    result = analysis.pipeline.collect_assets()["samples"]
    assert os.path.exists(result)
    import bilby

    posterior = bilby.result.read_in_result(result)
    assert sorted(posterior.search_parameter_keys) == ["h0", "iota", "phi0", "psi"]
    assert len(posterior.posterior) > 10
