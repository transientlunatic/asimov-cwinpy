"""The ``cwinpy-run`` strategy: one blueprint for a whole run."""

import copy
import os

import pytest
import yaml

from asimov.strategies import StrategyContext, StrategyError, read_group, strategy_plugins
from asimov_cwinpy.strategy import RunStrategy, pulsar_name, shard_ranges

START = 1000000000
DAY = 86400


@pytest.fixture
def pars(tmp_path):
    """Three ephemerides, and a function that makes another."""

    def make(name, text=None):
        path = tmp_path / f"{name}.par"
        path.write_text(text or f"PSRJ {name}\nF0 10.0\nPEPOCH 55000\n")
        return str(path)

    return {name: make(name) for name in ("J0001+0001", "J0002+0002", "J0003+0003")} | {
        "make": make
    }


def spec(pars, **changes):
    found = {
        "type": "cwinpy-run",
        "interferometers": ["H1", "L1"],
        "data start": START,
        "data end": START + 10 * DAY,
        "shards": {"count": 5},
        "pulsars": [pars["J0001+0001"], pars["J0002+0002"]],
    }
    found.update(changes)
    return found


def expand(spec_, name="O4a"):
    return RunStrategy().expand(
        {"kind": "analysis", "name": name, "strategy": spec_}, StrategyContext()
    )


def by_name(documents):
    return {(d["kind"], d.get("event"), d["name"]): d for d in documents}


class TestShards:
    def test_whole_jobs_in_each_shard(self):
        # ten days in five shards: two jobs, so two days, each
        ranges = shard_ranges(START, START + 10 * DAY, DAY, {"count": 5})
        assert ranges == [(START + 2 * i * DAY, START + 2 * (i + 1) * DAY) for i in range(5)]

    def test_the_last_shard_is_short(self):
        ranges = shard_ranges(START, START + 10 * DAY + 100, DAY, {"count": 3})
        # 11 jobs, four to a shard: 4 + 4 + 3
        assert [b - a for a, b in ranges] == [4 * DAY, 4 * DAY, 2 * DAY + 100]
        assert ranges[0][0] == START and ranges[-1][1] == START + 10 * DAY + 100

    def test_a_part_of_a_job_at_the_end_is_a_job(self):
        # 10 days and a bit is 11 jobs; to ask for 10 shards takes two jobs for each
        ranges = shard_ranges(START, START + 10 * DAY + 100, DAY, {"count": 10})
        assert [b - a for a, b in ranges] == [2 * DAY] * 5 + [100]

    def test_the_shards_are_contiguous_and_cover_the_span(self):
        ranges = shard_ranges(START, START + 37 * DAY + 5, DAY, {"count": 7})
        assert ranges[0][0] == START and ranges[-1][1] == START + 37 * DAY + 5
        assert all(a[1] == b[0] for a, b in zip(ranges, ranges[1:]))

    def test_no_more_shards_than_jobs(self):
        assert len(shard_ranges(START, START + 3 * DAY, DAY, {"count": 50})) == 3

    def test_by_length_rounds_up_to_whole_jobs(self):
        ranges = shard_ranges(START, START + 10 * DAY, DAY, {"length": 3 * DAY - 1})
        assert [b - a for a, b in ranges] == [3 * DAY, 3 * DAY, 3 * DAY, DAY]

    def test_by_length_is_at_least_one_job(self):
        assert len(shard_ranges(START, START + 3 * DAY, DAY, {"length": 1})) == 3

    def test_joblength_sets_the_unit(self):
        ranges = shard_ranges(START, START + 8000, 1000, {"count": 2})
        assert ranges == [(START, START + 4000), (START + 4000, START + 8000)]


class TestPulsarName:
    def test_psrj(self, pars):
        assert pulsar_name(pars["J0001+0001"]) == "J0001+0001"

    def test_psrj_is_preferred_to_the_others(self, pars):
        path = pars["make"]("x", "NAME B0001+00\nPSR B0001+00\nPSRJ J0001+0001\n")
        assert pulsar_name(path) == "J0001+0001"

    def test_falls_back_to_psr_then_name(self, pars):
        assert pulsar_name(pars["make"]("a", "PSR B1\nNAME C1\n")) == "B1"
        assert pulsar_name(pars["make"]("b", "NAME C1\n")) == "C1"

    def test_lower_case_and_tabs(self, pars):
        assert pulsar_name(pars["make"]("c", "psrj\tJ1\n")) == "J1"

    def test_a_file_with_no_name(self, pars):
        with pytest.raises(StrategyError, match="does not name its pulsar"):
            pulsar_name(pars["make"]("d", "F0 1\n"))

    def test_a_file_which_is_not_there(self, tmp_path):
        with pytest.raises(StrategyError, match="cannot be read"):
            pulsar_name(str(tmp_path / "missing.par"))


class TestValidation:
    def check(self, pars, message, **changes):
        with pytest.raises(StrategyError, match=message):
            RunStrategy().validate(spec(pars, **changes))

    def test_a_good_spec(self, pars):
        RunStrategy().validate(spec(pars))

    @pytest.mark.parametrize(
        "key", ["interferometers", "data start", "data end", "shards", "pulsars"]
    )
    def test_what_is_needed(self, pars, key):
        bad = spec(pars)
        del bad[key]
        with pytest.raises(StrategyError, match=f"needs '{key}'"):
            RunStrategy().validate(bad)

    def test_unknown_option(self, pars):
        self.check(pars, "Unknown option.*shard", shard={"count": 1})

    def test_the_end_is_after_the_start(self, pars):
        self.check(pars, "after 'data start'", **{"data end": START})

    def test_gps_times_are_integers(self, pars):
        self.check(pars, "GPS time", **{"data start": "soon"})
        self.check(pars, "GPS time", **{"data start": True})

    def test_shards_is_one_of_count_or_length(self, pars):
        self.check(pars, "count: n", shards={"count": 2, "length": 5})
        self.check(pars, "count: n", shards={"size": 2})
        self.check(pars, "count: n", shards=5)

    def test_shards_is_positive(self, pars):
        self.check(pars, "whole number", shards={"count": 0})
        self.check(pars, "whole number", shards={"length": 1.5})

    def test_detectors_are_different(self, pars):
        self.check(pars, "different detectors", interferometers=["H1", "H1"])

    def test_factors_are_numbers(self, pars):
        self.check(pars, "list of numbers", **{"frequency factors": ["two"]})
        self.check(pars, "list of numbers", **{"frequency factors": []})

    def test_a_block_has_only_known_options(self, pars):
        self.check(pars, "Unknown option.*'heterodyne': retries", heterodyne={"retries": 3})
        self.check(pars, "Unknown option.*'pe': nonsense", pe={"nonsense": 1})
        self.check(pars, "Unknown option.*'merge': data", merge={"data": {}})

    def test_a_block_is_a_mapping(self, pars):
        self.check(pars, "must be a mapping", pe=[1])

    def test_the_same_pulsar_twice(self, pars):
        self.check(
            pars, "J0001\\+0001 is listed twice", pulsars=[pars["J0001+0001"], pars["J0001+0001"]]
        )

    def test_the_same_pulsar_in_two_batches(self, pars):
        self.check(
            pars,
            "batches main and late",
            pulsars=[pars["J0001+0001"], {"ephemeris": pars["J0001+0001"], "batch": "late"}],
        )

    def test_names_that_are_not_safe(self, pars):
        self.check(
            pars,
            "batch name",
            pulsars=[{"ephemeris": pars["J0001+0001"], "batch": "a/b"}],
        )
        self.check(
            pars,
            "pulsar name",
            pulsars=[{"ephemeris": pars["J0001+0001"], "name": "../x"}],
        )

    def test_an_entry_that_is_not_understood(self, pars):
        self.check(pars, "file name, or a directory of them", pulsars=[3])
        self.check(
            pars, "file name, or a directory of them", pulsars=[{"ephemeris": "x", "color": 1}]
        )
        self.check(pars, "needs its 'ephemeris'", pulsars=[{"batch": "x"}])

    def test_a_missing_ephemeris_is_found_at_validation(self, pars, tmp_path):
        self.check(pars, "cannot be read", pulsars=[str(tmp_path / "nope.par")])

    def test_it_is_registered(self):
        assert strategy_plugins()["cwinpy-run"] is RunStrategy


class TestExpansion:
    def test_the_documents(self, pars):
        found = by_name(expand(spec(pars)))
        kinds = sorted(found)
        shards = [k for k in kinds if k[0] == "analysis" and k[1] == "O4a-heterodyne"]
        assert [k[2] for k in shards] == [
            "merge",
            "shard-0001",
            "shard-0002",
            "shard-0003",
            "shard-0004",
            "shard-0005",
        ]
        assert ("event", None, "O4a-heterodyne") in found
        for pulsar in ("J0001+0001", "J0002+0002"):
            assert ("event", None, pulsar) in found
            assert ("analysis", pulsar, "pe") in found
        assert len(found) == 1 + 6 + 2 * 2

    def test_a_subject_comes_before_its_analyses(self, pars):
        documents = expand(spec(pars))
        subjects = set()
        for document in documents:
            if document["kind"] == "event":
                subjects.add(document["name"])
            else:
                assert document["event"] in subjects

    def test_the_names_do_not_change(self, pars):
        assert expand(spec(pars)) == expand(spec(pars))

    def test_the_run_subject_is_named_by_the_blueprint_or_by_run(self, pars):
        assert expand(spec(pars))[0]["name"] == "O4a-heterodyne"
        assert expand(spec(pars, run="O4a-het"))[0]["name"] == "O4a-het"
        assert expand(spec(pars, run="X"), name="other")[0]["name"] == "X"

    def test_shards_cover_the_span_without_a_gap(self, pars):
        shards = [d for d in expand(spec(pars)) if d["name"].startswith("shard-")]
        settings = [d["cwinpy-heterodyne"] for d in shards]
        assert settings[0]["data start"] == START
        assert settings[-1]["data end"] == START + 10 * DAY
        assert all(a["data end"] == b["data start"] for a, b in zip(settings, settings[1:]))

    def test_every_shard_heterodynes_every_pulsar_of_its_batch(self, pars):
        shards = [d for d in expand(spec(pars)) if d["name"].startswith("shard-")]
        for shard in shards:
            assert shard["cwinpy-heterodyne"]["ephemerides"] == [
                pars["J0001+0001"],
                pars["J0002+0002"],
            ]

    def test_the_merge_needs_every_shard_and_nothing_else(self, pars):
        documents = expand(spec(pars))
        merge = next(d for d in documents if d["name"] == "merge")
        assert merge["needs"] == [f"shard-000{i}" for i in range(1, 6)]
        assert merge["pipeline"] == "cwinpy-merge"

    def test_pe_needs_the_merge_of_the_run_subject(self, pars):
        pe = next(d for d in expand(spec(pars)) if d["name"] == "pe")
        assert pe["needs"] == ["O4a-heterodyne/merge"]
        assert pe["pipeline"] == "cwinpy-pe"

    def test_detectors_and_factors_reach_every_subject(self, pars):
        documents = expand(spec(pars, **{"frequency factors": [2]}))
        for document in documents:
            if document["kind"] == "event":
                assert document["interferometers"] == ["H1", "L1"]
                assert document["frequency factors"] == [2]

    def test_factors_default_to_one_and_two(self, pars):
        assert expand(spec(pars))[0]["frequency factors"] == [1, 2]

    def test_the_pulsar_subject_has_its_ephemeris(self, pars):
        subject = next(d for d in expand(spec(pars)) if d["name"] == "J0002+0002")
        assert subject["ephemeris"] == pars["J0002+0002"]

    def test_a_name_can_be_given(self, pars):
        documents = expand(spec(pars, pulsars=[{"ephemeris": pars["J0001+0001"], "name": "crab"}]))
        assert ("analysis", "crab", "pe") in by_name(documents)

    def test_joblength_defaults_to_a_day_and_is_passed_on(self, pars):
        shard = next(d for d in expand(spec(pars)) if d["name"] == "shard-0001")
        assert shard["cwinpy-heterodyne"]["joblength"] == DAY
        shard = next(d for d in expand(spec(pars, joblength=3600)) if d["name"] == "shard-0001")
        assert shard["cwinpy-heterodyne"]["joblength"] == 3600

    def test_the_settings_go_where_each_pipeline_reads_them(self, pars):
        data = {"channels": {"H1": "H1:X"}, "datafind url": "d.example"}
        documents = by_name(
            expand(
                spec(
                    pars,
                    heterodyne={
                        "data": data,
                        "settings": {"strict data requirement": False, "segment server": "s"},
                        "scheduler": {"request memory": "16GB"},
                        "retry": {"attempts": 10, "delay": 3600},
                    },
                    merge={"scheduler": {"request memory": "4GB"}, "retry": {"attempts": 3}},
                    pe={
                        "sampler": {"sampler": "dynesty"},
                        "priors": {"h0": {"type": "uniform", "minimum": 0, "maximum": 1}},
                        "settings": {"fake seed": 1},
                        "scheduler": {"request memory": "8GB"},
                        "retry": {"attempts": 2},
                    },
                )
            )
        )
        shard = documents[("analysis", "O4a-heterodyne", "shard-0001")]
        assert shard["data"] == data
        assert shard["scheduler"] == {"request memory": "16GB"}
        assert shard["retry"] == {"attempts": 10, "delay": 3600}
        assert shard["cwinpy-heterodyne"]["strict data requirement"] is False
        assert shard["cwinpy-heterodyne"]["segment server"] == "s"
        merge = documents[("analysis", "O4a-heterodyne", "merge")]
        assert merge["scheduler"] == {"request memory": "4GB"} and merge["retry"] == {"attempts": 3}
        assert "data" not in merge
        pe = documents[("analysis", "J0001+0001", "pe")]
        assert pe["sampler"] == {"sampler": "dynesty"}
        assert pe["cwinpy-pe"] == {"fake seed": 1}
        assert pe["scheduler"] == {"request memory": "8GB"} and pe["retry"] == {"attempts": 2}
        assert documents[("event", None, "J0001+0001")]["priors"]["h0"]["maximum"] == 1
        assert "data" not in pe and "sampler" not in shard

    def test_nothing_is_shared_between_documents(self, pars):
        documents = expand(
            spec(pars, heterodyne={"retry": {"attempts": 10}}, pe={"retry": {"attempts": 2}})
        )
        shards = [d for d in documents if d["name"].startswith("shard-")]
        shards[0]["retry"]["attempts"] = 99
        shards[0]["cwinpy-heterodyne"]["ephemerides"].append("x")
        assert shards[1]["retry"]["attempts"] == 10
        assert len(shards[1]["cwinpy-heterodyne"]["ephemerides"]) == 2

    def test_the_blueprint_is_not_changed(self, pars):
        blueprint = {"kind": "analysis", "name": "O4a", "strategy": spec(pars)}
        before = copy.deepcopy(blueprint)
        RunStrategy().expand(blueprint, StrategyContext())
        assert blueprint == before


class TestBatches:
    def late(self, pars):
        return spec(
            pars,
            pulsars=[
                pars["J0001+0001"],
                pars["J0002+0002"],
                {"ephemeris": pars["J0003+0003"], "batch": "late"},
            ],
        )

    def test_the_first_batch_is_unchanged_by_adding_one(self, pars):
        before = {(d["kind"], d.get("event"), d["name"]): d for d in expand(spec(pars))}
        after = by_name(expand(self.late(pars)))
        for key, document in before.items():
            assert after[key] == document

    def test_a_batch_has_shards_and_a_merge_of_its_own(self, pars):
        found = by_name(expand(self.late(pars)))
        merge = found[("analysis", "O4a-heterodyne", "merge-late")]
        assert merge["needs"] == [f"late-shard-000{i}" for i in range(1, 6)]
        for i in range(1, 6):
            shard = found[("analysis", "O4a-heterodyne", f"late-shard-000{i}")]
            assert shard["cwinpy-heterodyne"]["ephemerides"] == [pars["J0003+0003"]]

    def test_the_batch_shards_read_the_same_stretches(self, pars):
        found = by_name(expand(self.late(pars)))
        for i in range(1, 6):
            main = found[("analysis", "O4a-heterodyne", f"shard-000{i}")]["cwinpy-heterodyne"]
            late = found[("analysis", "O4a-heterodyne", f"late-shard-000{i}")]["cwinpy-heterodyne"]
            assert (main["data start"], main["data end"]) == (late["data start"], late["data end"])

    def test_a_pulsar_needs_the_merge_of_its_batch(self, pars):
        found = by_name(expand(self.late(pars)))
        assert found[("analysis", "J0003+0003", "pe")]["needs"] == ["O4a-heterodyne/merge-late"]
        assert found[("analysis", "J0001+0001", "pe")]["needs"] == ["O4a-heterodyne/merge"]

    def test_the_order_of_the_batches_does_not_matter(self, pars):
        one = self.late(pars)
        other = copy.deepcopy(one)
        other["pulsars"] = [other["pulsars"][2]] + other["pulsars"][:2]
        assert sorted(map(str, expand(one))) == sorted(map(str, expand(other)))


def load(text):
    return list(yaml.safe_load_all(text))


class TestSummary:
    def names(self, documents):
        return [(d["kind"], d.get("event"), d["name"]) for d in documents]

    def test_there_are_no_summary_pages_unless_asked_for(self, pars):
        assert ("analysis", "O4a-heterodyne", "summary") not in self.names(expand(spec(pars)))
        for off in (False, None):
            assert ("analysis", "O4a-heterodyne", "summary") not in self.names(
                expand(spec(pars, summary=off))
            )

    def test_true_asks_for_the_defaults(self, pars):
        found = by_name(expand(spec(pars, summary=True)))
        summary = found[("analysis", "O4a-heterodyne", "summary")]
        assert summary["pipeline"] == "cwinpy-summary"
        assert "cwinpy-summary" not in summary and "retry" not in summary

    def test_it_needs_the_parameter_estimation_of_every_pulsar(self, pars):
        summary = by_name(expand(spec(pars, summary=True)))[
            ("analysis", "O4a-heterodyne", "summary")
        ]
        assert summary["needs"] == ["J0001+0001/pe", "J0002+0002/pe"]

    def test_it_needs_those_of_the_pulsars_in_every_batch_in_order(self, pars):
        pulsars = [
            {"ephemeris": pars["J0003+0003"], "batch": "late"},
            pars["J0002+0002"],
            pars["J0001+0001"],
        ]
        summary = by_name(expand(spec(pars, pulsars=pulsars, summary=True)))[
            ("analysis", "O4a-heterodyne", "summary")
        ]
        assert summary["needs"] == ["J0001+0001/pe", "J0002+0002/pe", "J0003+0003/pe"]

    def test_it_is_the_last_document(self, pars):
        assert expand(spec(pars, summary={}))[-1]["name"] == "summary"

    def test_the_settings_go_where_the_pipeline_reads_them(self, pars):
        asked = {
            "settings": {"time series": False, "sort by": "F0"},
            "scheduler": {"request memory": "8GB"},
            "retry": {"attempts": 2},
        }
        summary = by_name(expand(spec(pars, summary=asked)))[
            ("analysis", "O4a-heterodyne", "summary")
        ]
        assert summary["cwinpy-summary"] == {"time series": False, "sort by": "F0"}
        assert summary["scheduler"] == {"request memory": "8GB"}
        assert summary["retry"] == {"attempts": 2}

    def test_nothing_else_changes(self, pars):
        without = expand(spec(pars))
        with_summary = expand(spec(pars, summary=True))
        assert with_summary[:-1] == without

    def test_an_option_that_it_does_not_have_is_refused(self, pars):
        with pytest.raises(StrategyError, match="Unknown option.*'summary': data"):
            RunStrategy().validate(spec(pars, summary={"data": {}}))

    def test_it_is_a_mapping_or_a_flag(self, pars):
        with pytest.raises(StrategyError, match="must be a mapping"):
            RunStrategy().validate(spec(pars, summary=["x"]))
        RunStrategy().validate(spec(pars, summary=True))
        RunStrategy().validate(spec(pars, summary={"retry": {"attempts": 1}}))

    def test_the_documents_name_nothing_twice_whatever_the_batch(self, pars):
        pulsars = [pars["J0001+0001"], {"ephemeris": pars["J0003+0003"], "batch": "summary"}]
        names = self.names(expand(spec(pars, pulsars=pulsars, summary=True)))
        assert len(names) == len(set(names))


@pytest.fixture
def atnf(monkeypatch, tmp_path):
    """The ATNF catalogue (it says which pulsars were asked for); the project is tmp_path."""
    from asimov_cwinpy import ephemerides

    monkeypatch.chdir(tmp_path)
    calls = []

    def fetch(name):
        calls.append(name)
        if name == "J9999+9999":
            raise StrategyError("The ATNF catalogue has no pulsar J9999+9999.")
        return f"PSRJ {name}\nF0 10\nPEPOCH 55000\n", "2.9.0", "1.3.3"

    monkeypatch.setattr(ephemerides, "fetch_atnf", fetch)
    return calls


def crab(**changes):
    return {"name": "J0534+2200", "source": "atnf", **changes}


class TestDirectories:
    def directory(self, tmp_path, *names, where="pulsars"):
        root = tmp_path / where
        for name in names:
            path = root / f"{name}.par"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"PSRJ {name.split('/')[-1]}\nF0 10\n")
        return str(root)

    def test_every_pulsar_in_a_directory_is_a_pulsar_named_by_its_psrj(self, pars, tmp_path):
        root = self.directory(tmp_path, "b-file", "a-file")
        # the names of the files are not the names of the pulsars
        (tmp_path / "pulsars" / "a-file.par").write_text("PSRJ J0001+0001\n")
        (tmp_path / "pulsars" / "b-file.par").write_text("PSRJ J0002+0002\n")
        found = by_name(expand(spec(pars, pulsars=[root])))
        assert ("analysis", "J0001+0001", "pe") in found and (
            "analysis",
            "J0002+0002",
            "pe",
        ) in found
        assert found[("event", None, "J0001+0001")]["ephemeris"] == os.path.join(root, "a-file.par")

    def test_the_directories_under_it_are_searched(self, pars, tmp_path):
        root = self.directory(tmp_path, "J0001+0001", "deeper/J0002+0002", "deeper/more/J0003+0003")
        found = by_name(expand(spec(pars, pulsars=[root])))
        assert {n for (k, e, n) in found if k == "event" and n.startswith("J0")} == {
            "J0001+0001",
            "J0002+0002",
            "J0003+0003",
        }

    def test_the_pulsars_are_in_the_order_of_their_paths(self, pars, tmp_path):
        root = self.directory(tmp_path, "J0003+0003", "J0001+0001", "J0002+0002")
        shard = next(d for d in expand(spec(pars, pulsars=[root])) if d["name"] == "shard-0001")
        assert [os.path.basename(f) for f in shard["cwinpy-heterodyne"]["ephemerides"]] == [
            "J0001+0001.par",
            "J0002+0002.par",
            "J0003+0003.par",
        ]

    def test_a_directory_is_a_batch_like_any_other_entry(self, pars, tmp_path):
        root = self.directory(tmp_path, "J0003+0003", where="late")
        found = by_name(
            expand(spec(pars, pulsars=[pars["J0001+0001"], {"ephemeris": root, "batch": "late"}]))
        )
        assert ("analysis", "O4a-heterodyne", "late-shard-0001") in found
        assert found[("analysis", "J0003+0003", "pe")]["needs"] == ["O4a-heterodyne/merge-late"]
        assert found[("analysis", "J0001+0001", "pe")]["needs"] == ["O4a-heterodyne/merge"]

    def test_a_directory_and_a_file_together(self, pars, tmp_path):
        root = self.directory(tmp_path, "J0003+0003")
        found = by_name(expand(spec(pars, pulsars=[pars["J0001+0001"], root])))
        assert {n for (k, e, n) in found if k == "analysis" and n == "pe"} == {"pe"}
        assert ("event", None, "J0001+0001") in found and ("event", None, "J0003+0003") in found

    def test_a_pulsar_in_a_directory_and_listed_again_is_listed_twice(self, pars, tmp_path):
        root = self.directory(tmp_path, "J0001+0001")
        with pytest.raises(StrategyError, match=r"J0001\+0001 is listed twice"):
            RunStrategy().validate(spec(pars, pulsars=[root, pars["J0001+0001"]]))

    def test_a_name_cannot_be_given_to_a_directory(self, pars, tmp_path):
        root = self.directory(tmp_path, "J0003+0003")
        with pytest.raises(StrategyError, match="A name cannot be given to a directory"):
            RunStrategy().validate(spec(pars, pulsars=[{"ephemeris": root, "name": "x"}]))

    def test_a_directory_with_no_ephemeris_is_an_error_that_names_it(self, pars, tmp_path):
        (tmp_path / "empty").mkdir()
        with pytest.raises(StrategyError, match="no .par file in the directory .*empty"):
            RunStrategy().validate(spec(pars, pulsars=[str(tmp_path / "empty")]))

    def test_one_file_that_does_not_name_its_pulsar_stops_it_and_names_the_file(
        self, pars, tmp_path
    ):
        root = self.directory(tmp_path, "J0001+0001")
        (tmp_path / "pulsars" / "bad.par").write_text("F0 1\n")
        with pytest.raises(StrategyError, match="bad.par does not name its pulsar"):
            RunStrategy().validate(spec(pars, pulsars=[root]))

    def test_the_result_does_not_change_if_it_is_asked_again(self, pars, tmp_path):
        root = self.directory(tmp_path, "J0001+0001", "J0002+0002")
        assert expand(spec(pars, pulsars=[root])) == expand(spec(pars, pulsars=[root]))


class TestAtnf:
    def test_a_pulsar_by_name_is_fetched_into_the_ephemeris_directory_of_the_project(
        self, pars, atnf, tmp_path
    ):
        found = by_name(expand(spec(pars, pulsars=[crab()])))
        path = str(tmp_path / "ephemerides" / "J0534+2200.par")
        assert found[("event", None, "J0534+2200")]["ephemeris"] == path
        assert open(path).read().startswith("PSRJ J0534+2200")
        shard = found[("analysis", "O4a-heterodyne", "shard-0001")]
        assert shard["cwinpy-heterodyne"]["ephemerides"] == [path]
        assert atnf == ["J0534+2200"]

    def test_the_options_are_checked_without_the_network(self, pars, atnf, tmp_path):
        RunStrategy().validate(spec(pars, pulsars=[crab()]))
        assert atnf == [] and not (tmp_path / "ephemerides").exists()

    def test_it_is_fetched_once_however_often_it_is_expanded(self, pars, atnf):
        first = expand(spec(pars, pulsars=[crab()]))
        assert expand(spec(pars, pulsars=[crab()])) == first
        assert atnf == ["J0534+2200"]

    def test_the_directory_can_be_chosen(self, pars, atnf, tmp_path):
        found = by_name(
            expand(spec(pars, pulsars=[crab()], **{"ephemeris directory": "timing/atnf"}))
        )
        path = str(tmp_path / "timing" / "atnf" / "J0534+2200.par")
        assert found[("event", None, "J0534+2200")]["ephemeris"] == path and os.path.exists(path)
        elsewhere = str(tmp_path / "elsewhere")
        found = by_name(expand(spec(pars, pulsars=[crab()], **{"ephemeris directory": elsewhere})))
        assert found[("event", None, "J0534+2200")]["ephemeris"] == elsewhere + "/J0534+2200.par"

    def test_the_directory_is_a_path(self, pars, atnf):
        for bad in ("", 3, ["x"]):
            with pytest.raises(StrategyError, match="'ephemeris directory' must be a path"):
                RunStrategy().validate(spec(pars, pulsars=[crab()], **{"ephemeris directory": bad}))

    def test_a_file_that_is_there_is_used_and_not_fetched(self, pars, atnf, tmp_path):
        (tmp_path / "ephemerides").mkdir()
        (tmp_path / "ephemerides" / "J0534+2200.par").write_text(
            "PSRJ J0534+2200\nF0 29.9\n# mine\n"
        )
        expand(spec(pars, pulsars=[crab()]))
        assert atnf == []
        assert "# mine" in (tmp_path / "ephemerides" / "J0534+2200.par").read_text()

    def test_it_can_be_mixed_with_files_and_in_a_batch(self, pars, atnf):
        found = by_name(
            expand(
                spec(
                    pars,
                    pulsars=[
                        pars["J0001+0001"],
                        crab(batch="late"),
                        {"ephemeris": pars["J0002+0002"]},
                    ],
                )
            )
        )
        assert found[("analysis", "J0534+2200", "pe")]["needs"] == ["O4a-heterodyne/merge-late"]
        assert found[("analysis", "J0001+0001", "pe")]["needs"] == ["O4a-heterodyne/merge"]

    def test_it_needs_a_name(self, pars, atnf):
        with pytest.raises(StrategyError, match="ATNF catalogue needs its 'name'"):
            RunStrategy().validate(spec(pars, pulsars=[{"source": "atnf"}]))

    def test_it_has_no_ephemeris_of_its_own(self, pars, atnf):
        with pytest.raises(StrategyError, match="ATNF catalogue has no 'ephemeris'"):
            RunStrategy().validate(spec(pars, pulsars=[crab(ephemeris=pars["J0001+0001"])]))

    def test_there_are_two_sources(self, pars, atnf):
        with pytest.raises(
            StrategyError, match="source of a pulsar is 'file' or 'atnf', not 'web'"
        ):
            RunStrategy().validate(spec(pars, pulsars=[crab(source="web")]))
        RunStrategy().validate(
            spec(pars, pulsars=[{"ephemeris": pars["J0001+0001"], "source": "file"}])
        )

    def test_a_name_that_is_not_a_file_name_is_refused_before_anything_is_written(
        self, pars, atnf, tmp_path
    ):
        with pytest.raises(StrategyError, match="pulsar name '../x' is not usable"):
            RunStrategy().validate(spec(pars, pulsars=[crab(name="../x")]))
        with pytest.raises(StrategyError, match="is not usable"):
            expand(spec(pars, pulsars=[crab(name="../x")]))
        assert atnf == [] and not (tmp_path / "ephemerides").exists()

    def test_the_same_pulsar_by_name_and_in_a_file_is_listed_twice(self, pars, atnf):
        with pytest.raises(StrategyError, match=r"J0534\+2200 is listed twice"):
            RunStrategy().validate(
                spec(
                    pars, pulsars=[crab(), {"ephemeris": pars["J0001+0001"], "name": "J0534+2200"}]
                )
            )

    def test_a_pulsar_that_is_not_in_the_catalogue_stops_everything(self, pars, atnf):
        with pytest.raises(StrategyError, match="no pulsar J9999"):
            expand(spec(pars, pulsars=[crab(), crab(name="J9999+9999")]))

    def test_it_says_that_these_are_not_for_a_result(self, pars, atnf, caplog):
        expand(spec(pars, pulsars=[crab()]))
        assert "not timing solutions" in caplog.text and "not for a result" in caplog.text

    def test_it_says_nothing_if_none_are_from_the_catalogue(self, pars, atnf, caplog):
        expand(spec(pars))
        assert "ATNF" not in caplog.text


class TestEphemeridesApplied:
    def test_a_run_of_pulsars_by_name_and_in_a_directory_is_applied(
        self, project, atnf, tmp_path, pars
    ):
        directory = tmp_path / "timing"
        directory.mkdir()
        (directory / "J0003+0003.par").write_text("PSRJ J0003+0003\nF0 10\n")
        text = blueprint(pars, "J0001+0001").replace(
            f"    - {pars['J0001+0001']}\n",
            f"    - {{name: J0534+2200, source: atnf}}\n    - {directory}\n",
        )
        project.apply(text)
        assert set(project.open().subject_names()) == {"O4a-heterodyne", "J0534+2200", "J0003+0003"}
        meta = analyses(project, "J0534+2200")["pe"].meta
        assert meta["ephemeris"] == str(tmp_path / "ephemerides" / "J0534+2200.par")
        assert os.path.exists(meta["ephemeris"])
        shard = analyses(project, "O4a-heterodyne")["shard-0001"].meta["cwinpy-heterodyne"]
        assert len(shard["ephemerides"]) == 2

    def test_applying_it_again_does_not_fetch_again(self, project, atnf, pars):
        text = blueprint(pars, "J0001+0001").replace(
            f"    - {pars['J0001+0001']}\n", "    - {name: J0534+2200, source: atnf}\n"
        )
        project.apply(text)
        project.ledger = project.open()
        project.apply(text)
        assert atnf == ["J0534+2200"]

    def test_a_pulsar_that_is_not_in_the_catalogue_applies_nothing(self, project, atnf, pars):
        text = blueprint(pars, "J0001+0001").replace(
            f"    - {pars['J0001+0001']}\n", "    - {name: J9999+9999, source: atnf}\n"
        )
        with pytest.raises(StrategyError, match="no pulsar J9999"):
            project.apply(text)
        assert project.open().subject_names() == []


class TestSharedFilesystem:
    def documents(self, pars, **changes):
        return by_name(expand(spec(pars, summary=True, **changes)))

    def test_by_default_nothing_is_said_of_where_the_jobs_run_or_of_their_files(self, pars):
        found = self.documents(pars)
        for key in (
            ("analysis", "O4a-heterodyne", "merge"),
            ("analysis", "O4a-heterodyne", "summary"),
            ("analysis", "J0001+0001", "pe"),
        ):
            assert "htcondor" not in found[key] and "scheduler" not in found[key]

    def test_without_one_the_light_jobs_run_on_this_machine(self, pars):
        found = self.documents(pars, **{"shared filesystem": False})
        assert found[("analysis", "O4a-heterodyne", "merge")]["htcondor"] == {"universe": "local"}
        assert found[("analysis", "O4a-heterodyne", "summary")]["htcondor"] == {"universe": "local"}

    def test_without_one_the_parameter_estimation_has_its_files_transferred(self, pars):
        found = self.documents(pars, **{"shared filesystem": False})
        for pulsar in ("J0001+0001", "J0002+0002"):
            assert found[("analysis", pulsar, "pe")]["scheduler"] == {"transfer files": True}

    def test_the_shards_are_told_that_the_nodes_do_not_see_the_project(self, pars):
        shared = self.documents(pars)
        without = self.documents(pars, **{"shared filesystem": False})
        for number in range(1, 6):
            key = ("analysis", "O4a-heterodyne", f"shard-000{number}")
            assert (
                "scheduler" not in shared[key] or "transfer files" not in shared[key]["scheduler"]
            )
            assert without[key]["scheduler"]["transfer files"] is True

    def test_saying_that_there_is_one_is_the_default(self, pars):
        assert self.documents(pars, **{"shared filesystem": True}) == self.documents(pars)

    def test_what_the_blueprint_says_of_a_job_is_kept_and_added_to(self, pars):
        found = self.documents(
            pars,
            **{
                "shared filesystem": False,
                "merge": {
                    "scheduler": {"request memory": "8GB"},
                    "htcondor": {"requirements": "R"},
                },
                "pe": {"scheduler": {"request memory": "4GB", "environment": "/cvmfs/x"}},
            },
        )
        merge = found[("analysis", "O4a-heterodyne", "merge")]
        assert merge["htcondor"] == {"universe": "local", "requirements": "R"}
        assert merge["scheduler"] == {"request memory": "8GB"}
        pe = found[("analysis", "J0001+0001", "pe")]
        assert pe["scheduler"] == {
            "transfer files": True,
            "request memory": "4GB",
            "environment": "/cvmfs/x",
        }

    def test_what_the_blueprint_says_overrides_the_default(self, pars):
        found = self.documents(
            pars,
            **{
                "shared filesystem": False,
                "merge": {"htcondor": {"universe": "vanilla"}},
                "pe": {"scheduler": {"transfer files": False}},
            },
        )
        assert found[("analysis", "O4a-heterodyne", "merge")]["htcondor"] == {"universe": "vanilla"}
        assert found[("analysis", "J0001+0001", "pe")]["scheduler"] == {"transfer files": False}

    def test_the_shards_may_have_an_environment_and_requirements_of_their_own(self, pars):
        found = self.documents(
            pars,
            heterodyne={
                "scheduler": {"environment": "/cvmfs/software.igwn.org/conda/envs/igwn"},
                "htcondor": {"requirements": "(HAS_CVMFS_software_igwn_org =?= True)"},
            },
        )
        shard = found[("analysis", "O4a-heterodyne", "shard-0001")]
        assert shard["scheduler"]["environment"].endswith("envs/igwn")
        assert shard["htcondor"]["requirements"].startswith("(HAS_CVMFS")

    def test_it_is_true_or_false(self, pars):
        for bad in ("no", 0, None, []):
            with pytest.raises(StrategyError, match="'shared filesystem' must be true or false"):
                RunStrategy().validate(spec(pars, **{"shared filesystem": bad}))

    def test_nothing_is_shared_between_the_documents(self, pars):
        found = self.documents(pars, **{"shared filesystem": False})
        found[("analysis", "J0001+0001", "pe")]["scheduler"]["x"] = 1
        assert "x" not in found[("analysis", "J0002+0002", "pe")]["scheduler"]


# The blueprint, applied by asimov to a project

BLUEPRINT = """\
kind: analysis
name: O4a
strategy:
  type: cwinpy-run
  interferometers: [H1, L1]
  data start: {start}
  data end: {end}
  shards: {{count: 4}}
  pulsars:
{pulsars}
  heterodyne:
    data:
      channels: {{H1: "H1:GDS-CALIB_STRAIN", L1: "L1:GDS-CALIB_STRAIN"}}
      frame types: {{H1: H1_HOFT_C00, L1: L1_HOFT_C00}}
      segments: {{H1: "H1:DMT-ANALYSIS_READY:1", L1: "L1:DMT-ANALYSIS_READY:1"}}
      datafind url: datafind.example.org
    retry: {{attempts: 10, delay: 3600}}
    scheduler: {{request memory: 16384MB}}
  pe:
    sampler: {{sampler: dynesty}}
    priors:
      h0: {{type: uniform, minimum: 0.0, maximum: 1.0e-22}}
"""


def blueprint(pars, *names, extra=""):
    lines = []
    for name in names:
        if isinstance(name, tuple):
            lines.append(f"    - {{ephemeris: {pars[name[0]]}, batch: {name[1]}}}")
        else:
            lines.append(f"    - {pars[name]}")
    return BLUEPRINT.format(start=START, end=START + 8 * DAY, pulsars="\n".join(lines)) + extra


def analyses(project, subject):
    return {a.name: a for a in project.open().get_event(subject)[0].productions}


class TestSplitDetectors:
    def documents(self, pars, **changes):
        return by_name(expand(spec(pars, **changes)))

    def shards(self, found):
        return {k[2]: v for k, v in found.items() if k[2].startswith("shard-")}

    def test_by_default_a_shard_has_all_the_detectors(self, pars):
        shards = self.shards(self.documents(pars))
        assert sorted(shards) == [f"shard-000{i}" for i in range(1, 6)]
        assert all("interferometers" not in shard for shard in shards.values())

    def test_split_there_is_a_shard_of_each_detector_for_each_span(self, pars):
        shards = self.shards(self.documents(pars, **{"split detectors": True}))
        assert len(shards) == 10
        assert shards["shard-0003-H1"]["interferometers"] == ["H1"]
        assert shards["shard-0003-L1"]["interferometers"] == ["L1"]

    def test_the_detectors_of_a_span_have_the_same_span_and_settings(self, pars):
        shards = self.shards(self.documents(pars, **{"split detectors": True}))
        h1, l1 = shards["shard-0002-H1"], shards["shard-0002-L1"]
        assert h1["cwinpy-heterodyne"] == l1["cwinpy-heterodyne"]
        assert {k: v for k, v in h1.items() if k not in ("name", "interferometers")} == {
            k: v for k, v in l1.items() if k not in ("name", "interferometers")
        }

    def test_the_merge_needs_every_one_of_them(self, pars):
        found = self.documents(pars, **{"split detectors": True})
        merge = found[("analysis", "O4a-heterodyne", "merge")]
        assert len(merge["needs"]) == 10
        assert set(merge["needs"]) == set(self.shards(found))

    def test_the_event_keeps_all_the_detectors(self, pars):
        found = self.documents(pars, **{"split detectors": True})
        assert found[("event", None, "O4a-heterodyne")]["interferometers"] == ["H1", "L1"]
        assert found[("event", None, "J0001+0001")]["interferometers"] == ["H1", "L1"]

    def test_one_detector_is_not_split(self, pars):
        shards = self.shards(
            self.documents(pars, interferometers=["H1"], **{"split detectors": True})
        )
        assert sorted(shards) == [f"shard-000{i}" for i in range(1, 6)]
        assert all("interferometers" not in shard for shard in shards.values())

    def test_it_is_true_or_false(self, pars):
        with pytest.raises(StrategyError, match="split detectors"):
            RunStrategy().validate(spec(pars, **{"split detectors": "yes"}))
        RunStrategy().validate(spec(pars, **{"split detectors": True}))


class TestApplied:
    def test_everything_is_made(self, project, pars):
        project.apply(blueprint(pars, "J0001+0001", "J0002+0002"))
        ledger = project.open()
        assert set(ledger.subject_names()) == {"O4a-heterodyne", "J0001+0001", "J0002+0002"}
        assert set(analyses(project, "O4a-heterodyne")) == {
            "shard-0001",
            "shard-0002",
            "shard-0003",
            "shard-0004",
            "merge",
        }
        assert set(analyses(project, "J0001+0001")) == {"pe"}

    def test_the_group_is_recorded(self, project, pars):
        project.apply(blueprint(pars, "J0001+0001"))
        group = read_group(project.open(), "O4a")
        assert group["type"] == "cwinpy-run"
        assert set(group["subjects"]) == {"O4a-heterodyne", "J0001+0001"}

    def test_the_analyses_are_of_the_right_pipelines_and_type(self, project, pars):
        project.apply(blueprint(pars, "J0001+0001"))
        shard = analyses(project, "O4a-heterodyne")["shard-0001"]
        assert shard.pipeline.name == "cwinpy-heterodyne"
        assert shard.pipeline.analysis_type == "cwinpy"
        assert analyses(project, "O4a-heterodyne")["merge"].pipeline.name == "cwinpy-merge"
        assert analyses(project, "J0001+0001")["pe"].pipeline.name == "cwinpy-pe"

    def test_what_a_pipeline_reads_arrives(self, project, pars):
        project.apply(blueprint(pars, "J0001+0001", "J0002+0002"))
        shard = analyses(project, "O4a-heterodyne")["shard-0002"]
        meta = shard.meta
        assert meta["interferometers"] == ["H1", "L1"]
        assert meta["frequency factors"] == [1, 2]
        assert meta["data"]["frame types"]["H1"] == "H1_HOFT_C00"
        assert meta["retry"] == {"attempts": 10, "delay": 3600}
        assert meta["scheduler"]["request memory"] == "16384MB"
        settings = meta["cwinpy-heterodyne"]
        assert (settings["data start"], settings["data end"]) == (
            START + 2 * DAY,
            START + 4 * DAY,
        )
        assert settings["ephemerides"] == [pars["J0001+0001"], pars["J0002+0002"]]
        pe = analyses(project, "J0002+0002")["pe"].meta
        assert pe["ephemeris"] == pars["J0002+0002"]
        assert pe["interferometers"] == ["H1", "L1"]
        assert pe["sampler"] == {"sampler": "dynesty"}
        assert pe["priors"]["h0"]["maximum"] == 1.0e-22

    def test_a_shard_of_one_detector_analyses_only_that_one(self, project, pars):
        project.apply(blueprint(pars, "J0001+0001", extra="  split detectors: true\n"))
        found = analyses(project, "O4a-heterodyne")
        assert set(found) == {f"shard-000{i}-{d}" for i in range(1, 5) for d in ("H1", "L1")} | {
            "merge"
        }
        assert found["shard-0002-L1"].meta["interferometers"] == ["L1"]
        assert found["shard-0002-H1"].meta["interferometers"] == ["H1"]
        # the pipeline reads the detectors, and the segment and frame settings, from there
        assert found["shard-0002-L1"].pipeline.detectors() == ["L1"]
        assert analyses(project, "J0001+0001")["pe"].meta["interferometers"] == ["H1", "L1"]
        merge = found["merge"]
        assert len(merge.dependency_objects) == 8

    def test_the_needs_are_found(self, project, pars):
        project.apply(blueprint(pars, "J0001+0001"))
        merge = analyses(project, "O4a-heterodyne")["merge"]
        found = {dependency.name for dependency in merge.dependency_objects}
        assert found == {f"shard-000{i}" for i in range(1, 5)}
        pe = analyses(project, "J0001+0001")["pe"]
        assert [(d.event.name, d.name) for d in pe.dependency_objects] == [
            ("O4a-heterodyne", "merge")
        ]

    def test_applying_again_changes_nothing(self, project, pars):
        text = blueprint(pars, "J0001+0001", "J0002+0002")
        project.apply(text)
        project.set_status("O4a-heterodyne", "shard-0001", "finished")  # since then
        project.ledger = project.open()
        project.apply(text)
        assert analyses(project, "O4a-heterodyne")["shard-0001"].status == "finished"
        assert len(analyses(project, "O4a-heterodyne")) == 5

    def test_a_pulsar_added_later_is_a_batch_and_leaves_the_rest_alone(self, project, pars):
        project.apply(blueprint(pars, "J0001+0001", "J0002+0002"))
        project.set_status("O4a-heterodyne", "merge", "finished")
        project.ledger = project.open()
        project.apply(blueprint(pars, "J0001+0001", "J0002+0002", ("J0003+0003", "late")))
        run = analyses(project, "O4a-heterodyne")
        assert run["merge"].status == "finished"
        assert {n for n in run if n.startswith("late-shard-")} == {
            f"late-shard-000{i}" for i in range(1, 5)
        }
        assert run["merge-late"].status != "finished"
        pe = analyses(project, "J0003+0003")["pe"]
        assert [d.name for d in pe.dependency_objects] == ["merge-late"]

    def test_a_mistake_applies_nothing(self, project, pars):
        text = blueprint(pars, "J0001+0001").replace("shards: {count: 4}", "shards: {count: 0}")
        with pytest.raises(StrategyError, match="whole number"):
            project.apply(text)
        assert project.open().subject_names() == []

    def test_on_the_database_ledger(self, database_project, pars):
        database_project.apply(blueprint(pars, "J0001+0001", "J0002+0002"))
        assert len(analyses(database_project, "O4a-heterodyne")) == 5
        pe = analyses(database_project, "J0002+0002")["pe"]
        assert [(d.event.name, d.name) for d in pe.dependency_objects] == [
            ("O4a-heterodyne", "merge")
        ]


class TestSummaryApplied:
    def test_the_summary_is_made_and_its_needs_are_found(self, project, pars):
        text = blueprint(pars, "J0001+0001", "J0002+0002", extra="  summary: true\n")
        project.apply(text)
        summary = analyses(project, "O4a-heterodyne")["summary"]
        assert summary.pipeline.name == "cwinpy-summary"
        assert sorted((d.event.name, d.name) for d in summary.dependency_objects) == [
            ("J0001+0001", "pe"),
            ("J0002+0002", "pe"),
        ]

    def test_it_is_not_made_without_being_asked(self, project, pars):
        project.apply(blueprint(pars, "J0001+0001"))
        assert "summary" not in analyses(project, "O4a-heterodyne")

    def test_the_settings_arrive(self, project, pars):
        extra = "  summary:\n    settings: {time series: false}\n    retry: {attempts: 2}\n"
        project.apply(blueprint(pars, "J0001+0001", extra=extra))
        meta = analyses(project, "O4a-heterodyne")["summary"].meta
        assert meta["cwinpy-summary"] == {"time series": False}
        assert meta["retry"] == {"attempts": 2}
