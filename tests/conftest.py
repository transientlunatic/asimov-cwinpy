"""A scratch asimov project, so that the tests use real analyses and not mocks."""

import pytest

from asimov.analysis_types import reset_analysis_types
from asimov.cli.application import apply_page
from asimov.cli.project import make_project
from asimov.ledger import DatabaseLedger, YAMLLedger

PRIORS = """\
priors:
  h0: {type: uniform, minimum: 0.0, maximum: 1.0e-22}
  iota: {type: sine, minimum: 0.0, maximum: 3.141592653589793}
"""


class Project:
    """A project in a temporary directory, on the YAML ledger or the database one."""

    def __init__(self, root, engine="yamlfile"):
        self.root = root
        self.engine = engine
        make_project(name="cwinpy test", root=str(root), engine=engine)
        self.ledger = self.open()

    def open(self):
        """The ledger, read afresh from the file."""
        if self.engine == "yamlfile":
            return YAMLLedger(".asimov/ledger.yml")
        return DatabaseLedger(
            engine="sqlalchemy", location=f"sqlite:///{self.root}/.asimov/ledger.db"
        )

    def apply(self, text, subject=None):
        path = self.root / "blueprint.yaml"
        path.write_text(text)
        return apply_page(str(path), event=subject, ledger=self.ledger)

    def analysis(self, subject, name):
        loaded = self.open()
        return {a.name: a for a in loaded.get_event(subject)[0].productions}[name]

    def set_status(self, subject, name, status):
        """Set the status of an analysis, as asimov does when it submits one."""
        ledger = self.open()
        event = ledger.get_event(subject)[0]
        [a for a in event.productions if a.name == name][0].status = status
        ledger.update_event(event)
        ledger.save()


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A project, with the working directory in it."""
    reset_analysis_types()
    monkeypatch.chdir(tmp_path)
    yield Project(tmp_path)
    reset_analysis_types()


@pytest.fixture
def database_project(tmp_path, monkeypatch):
    """A project on the database ledger, which is the one asimov makes by default."""
    reset_analysis_types()
    monkeypatch.chdir(tmp_path)
    yield Project(tmp_path, engine="sqlite")
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


def shard_blueprint(
    cache=True,
    extra_analysis="",
    ephemerides="[/data/J0534+2200.par]",
    extra_event="",
    name="O4a-heterodyne",
):
    """A shard of a run, as a blueprint."""
    if cache:
        data = (
            "data:\n  channels: {H1: 'H1:FAKE-STRAIN', L1: 'L1:FAKE-STRAIN'}\n"
            "  cache files: {H1: /frames/H1, L1: /frames/L1}\n"
        )
        segments = "  segment lists: {H1: /segs/H1.txt, L1: /segs/L1.txt}\n"
    else:
        data = (
            "data:\n  channels: {H1: 'H1:GDS-CALIB_STRAIN', L1: 'L1:GDS-CALIB_STRAIN'}\n"
            "  frame types: {H1: H1_HOFT_C00, L1: L1_HOFT_C00}\n"
            "  segments: {H1: 'H1:DMT-ANALYSIS_READY:1', L1: 'L1:DMT-ANALYSIS_READY:1'}\n"
            "  datafind url: datafind.example.org:80\n  datafind url type: osdf\n"
        )
        segments = "  segment server: https://segments.example.org\n"
    return (
        f"kind: event\nname: {name}\ninterferometers: [H1, L1]\n{extra_event}---\n"
        f"kind: analysis\nname: shard-0001\npipeline: cwinpy-heterodyne\nevent: {name}\n"
        f"{data}cwinpy-heterodyne:\n  data start: 1000000000\n  data end: 1000002048\n"
        f"  ephemerides: {ephemerides}\n{segments}{extra_analysis}"
    )
