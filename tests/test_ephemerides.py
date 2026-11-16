"""Where the ephemerides come from: directories, and the ATNF catalogue (without the network)."""

import json
import os
import sys
import types

import pytest

from asimov.strategies import StrategyError

from asimov_cwinpy import ephemerides
from asimov_cwinpy.ephemerides import atnf_path, ensure_atnf, fetch_atnf, par_files, pulsar_name

EPHEMERIS = "NAME J0534+2200\nPSRJ J0534+2200\nF0 29.9\nPEPOCH 55000\n"


class FakeQuery:
    """What psrqpy's QueryATNF does that matters here."""

    calls = []
    catalogue = {"J0534+2200": EPHEMERIS}
    fail = None
    get_version = "2.9.0"

    def __init__(self, psrs):
        if FakeQuery.fail:
            raise FakeQuery.fail
        FakeQuery.calls.append(list(psrs))

    def get_ephemeris(self, name):
        return FakeQuery.catalogue.get(name)  # None for a pulsar that is not there


@pytest.fixture
def psrqpy(monkeypatch):
    FakeQuery.calls = []
    FakeQuery.fail = None
    FakeQuery.catalogue = {"J0534+2200": EPHEMERIS}
    module = types.ModuleType("psrqpy")
    module.QueryATNF = FakeQuery
    module.__version__ = "1.3.3"
    monkeypatch.setitem(sys.modules, "psrqpy", module)
    return FakeQuery


class TestPulsarName:
    def test_it_is_still_in_the_strategy_module(self):
        from asimov_cwinpy.strategy import pulsar_name as from_strategy

        assert from_strategy is pulsar_name

    def test_the_catalogues_ephemeris_names_its_pulsar(self, tmp_path):
        path = tmp_path / "x.par"
        path.write_text(EPHEMERIS)
        assert pulsar_name(str(path)) == "J0534+2200"


class TestParFiles:
    def make(self, root, *names):
        for name in names:
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"PSRJ {path.stem}\n")

    def test_every_par_file_is_found_in_the_order_of_its_path(self, tmp_path):
        self.make(tmp_path, "b.par", "a.par", "c.par")
        assert [os.path.basename(f) for f in par_files(str(tmp_path))] == [
            "a.par",
            "b.par",
            "c.par",
        ]

    def test_the_directories_under_it_are_searched_too(self, tmp_path):
        self.make(tmp_path, "top.par", "sub/inner.par", "sub/deeper/deepest.par")
        found = par_files(str(tmp_path))
        assert sorted(os.path.basename(f) for f in found) == ["deepest.par", "inner.par", "top.par"]

    def test_other_files_are_not_ephemerides(self, tmp_path):
        self.make(tmp_path, "a.par", "notes.txt", "a.par.bak", "a.atnf.json", "par")
        assert [os.path.basename(f) for f in par_files(str(tmp_path))] == ["a.par"]

    def test_a_directory_that_is_named_like_a_par_file_is_not_one(self, tmp_path):
        (tmp_path / "odd.par").mkdir()
        self.make(tmp_path, "real.par")
        assert [os.path.basename(f) for f in par_files(str(tmp_path))] == ["real.par"]

    def test_the_paths_keep_the_form_of_the_directory(self, tmp_path, monkeypatch):
        self.make(tmp_path, "pars/a.par")
        monkeypatch.chdir(tmp_path)
        assert par_files("pars") == [os.path.join("pars", "a.par")]

    def test_a_directory_with_none_is_an_error_that_names_it(self, tmp_path):
        self.make(tmp_path, "notes.txt")
        with pytest.raises(StrategyError, match="no .par file in the directory"):
            par_files(str(tmp_path))


class TestFetch:
    def test_the_text_and_the_versions_come_back(self, psrqpy):
        assert fetch_atnf("J0534+2200") == (EPHEMERIS, "2.9.0", "1.3.3")
        assert psrqpy.calls == [["J0534+2200"]]

    def test_a_pulsar_that_is_not_in_the_catalogue_is_an_error_that_names_it(self, psrqpy):
        with pytest.raises(StrategyError, match="ATNF catalogue has no pulsar J9999"):
            fetch_atnf("J9999+9999")

    def test_a_catalogue_that_cannot_be_reached_is_an_error_that_says_why(self, psrqpy):
        psrqpy.fail = ConnectionError("name resolution failed")
        with pytest.raises(StrategyError, match="could not be got.*name resolution failed"):
            fetch_atnf("J0534+2200")

    def test_psrqpy_not_being_installed_says_what_to_install(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "psrqpy", None)
        with pytest.raises(StrategyError, match="pip install psrqpy"):
            fetch_atnf("J0534+2200")


class TestEnsure:
    def test_it_is_written_as_the_name_with_the_par_extension(self, psrqpy, tmp_path):
        directory = tmp_path / "ephemerides"
        path = ensure_atnf("J0534+2200", str(directory))
        assert path == str(directory / "J0534+2200.par") == atnf_path("J0534+2200", str(directory))
        assert open(path).read() == EPHEMERIS
        assert os.path.isabs(path)

    def test_the_directory_is_made(self, psrqpy, tmp_path):
        ensure_atnf("J0534+2200", str(tmp_path / "a" / "b"))
        assert (tmp_path / "a" / "b" / "J0534+2200.par").exists()

    def test_a_relative_directory_gives_an_absolute_path(self, psrqpy, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert ensure_atnf("J0534+2200", "ephemerides") == str(
            tmp_path / "ephemerides" / "J0534+2200.par"
        )

    def test_it_says_where_the_ephemeris_came_from(self, psrqpy, tmp_path):
        ensure_atnf("J0534+2200", str(tmp_path))
        said = json.loads((tmp_path / "J0534+2200.atnf.json").read_text())
        assert said["pulsar"] == "J0534+2200"
        assert said["source"] == "ATNF pulsar catalogue"
        assert said["catalogue version"] == "2.9.0" and said["psrqpy"] == "1.3.3"
        assert said["fetched"].endswith("+00:00")  # UTC

    def test_the_text_ends_in_a_newline(self, psrqpy, tmp_path):
        psrqpy.catalogue = {"J0534+2200": "PSRJ J0534+2200\nF0 1"}
        assert open(ensure_atnf("J0534+2200", str(tmp_path))).read().endswith("\n")

    def test_a_file_that_is_there_is_used_and_nothing_is_fetched(self, psrqpy, tmp_path):
        (tmp_path / "J0534+2200.par").write_text("PSRJ J0534+2200\nF0 1\n# my timing solution\n")
        ensure_atnf("J0534+2200", str(tmp_path))
        assert psrqpy.calls == []
        assert "my timing solution" in (tmp_path / "J0534+2200.par").read_text()
        assert not (tmp_path / "J0534+2200.atnf.json").exists()

    def test_fetching_again_is_not_needed(self, psrqpy, tmp_path):
        ensure_atnf("J0534+2200", str(tmp_path))
        ensure_atnf("J0534+2200", str(tmp_path))
        assert psrqpy.calls == [["J0534+2200"]]

    def test_an_empty_file_is_fetched_again(self, psrqpy, tmp_path):
        (tmp_path / "J0534+2200.par").write_text("")
        ensure_atnf("J0534+2200", str(tmp_path))
        assert open(tmp_path / "J0534+2200.par").read() == EPHEMERIS

    def test_a_failure_leaves_no_file_to_be_taken_for_the_ephemeris(self, psrqpy, tmp_path):
        with pytest.raises(StrategyError):
            ensure_atnf("J9999+9999", str(tmp_path))
        assert os.listdir(tmp_path) == []

    def test_a_half_written_file_is_never_where_it_would_be_used(
        self, psrqpy, tmp_path, monkeypatch
    ):
        def broken(source, target):
            raise OSError("disk full")

        monkeypatch.setattr(ephemerides.os, "replace", broken)
        with pytest.raises(OSError):
            ensure_atnf("J0534+2200", str(tmp_path))
        assert not (tmp_path / "J0534+2200.par").exists()

    def test_it_logs_what_it_fetched(self, psrqpy, tmp_path, caplog):
        import logging

        with caplog.at_level(logging.INFO, logger="test"):
            ensure_atnf("J0534+2200", str(tmp_path), logging.getLogger("test"))
        assert "ATNF catalogue 2.9.0" in caplog.text
