"""A scratch asimov project, so that the tests use real analyses and not mocks."""

import pytest

from asimov.analysis_types import reset_analysis_types
from asimov.cli.application import apply_page
from asimov.cli.project import make_project
from asimov.ledger import YAMLLedger

PRIORS = """\
priors:
  h0: {type: uniform, minimum: 0.0, maximum: 1.0e-22}
  iota: {type: sine, minimum: 0.0, maximum: 3.141592653589793}
"""


class Project:
    """A project in a temporary directory."""

    def __init__(self, root):
        self.root = root
        make_project(name="cwinpy test", root=str(root), engine="yamlfile")
        self.ledger = YAMLLedger(".asimov/ledger.yml")

    def apply(self, text, subject=None):
        path = self.root / "blueprint.yaml"
        path.write_text(text)
        return apply_page(str(path), event=subject, ledger=self.ledger)

    def analysis(self, subject, name):
        loaded = YAMLLedger(".asimov/ledger.yml")
        return {a.name: a for a in loaded.get_event(subject)[0].productions}[name]


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A project, with the working directory in it."""
    reset_analysis_types()
    monkeypatch.chdir(tmp_path)
    yield Project(tmp_path)
    reset_analysis_types()


def pulsar_blueprint(extra_event="", extra_analysis="", name="J0534+2200"):
    """A pulsar and its parameter estimation, as a blueprint."""
    return (
        f"kind: event\nname: {name}\nephemeris: /data/{name}.par\n"
        f"interferometers: [H1, L1]\n{PRIORS}{extra_event}---\n"
        f"kind: analysis\nname: pe\npipeline: cwinpy-pe\nevent: {name}\n{extra_analysis}"
    )


SIMULATED = (
    "cwinpy-pe:\n  fake asd: {H1: 1.0e-24, L1: 1.0e-24}\n"
    "  fake start: 1000000000\n  fake end: 1000086400\n  fake dt: 60\n  fake seed: 1\n"
)
