"""The ``cwinpy-run`` strategy: one blueprint for a whole run."""

import copy

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
        self.check(pars, "file name, or a mapping", pulsars=[3])
        self.check(pars, "file name, or a mapping", pulsars=[{"ephemeris": "x", "color": 1}])
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
