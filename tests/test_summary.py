"""The cwinpy-summary pipeline, without cwinpy or HTCondor."""

import configparser
import ast
import json
import os
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from asimov.pipeline import PipelineException

from asimov_cwinpy import summary
from asimov_cwinpy.common import CwinpyError, TransientCwinpyError
from asimov_cwinpy.pe import PEPipeline
from asimov_cwinpy.summary import SummaryPipeline

PULSARS = ("J0534+2200", "J0537-6910")


@pytest.fixture(autouse=True)
def no_environment_capture():
    with patch.object(SummaryPipeline, "_capture_environment"):
        yield


@pytest.fixture
def files(tmp_path):
    """What the parameter estimations used: an ephemeris, and heterodyned data for each pulsar."""
    made = {}
    for pulsar in PULSARS:
        par = tmp_path / "in" / f"{pulsar}.par"
        par.parent.mkdir(exist_ok=True)
        par.write_text(f"PSRJ {pulsar}\nF0 10\n")
        data = {}
        for factor in (1, 2):
            data[factor] = {}
            for detector in ("H1", "L1"):
                path = tmp_path / "in" / f"merged_{pulsar}_{detector}_{factor}f.hdf5"
                path.write_text(f"{pulsar} {detector} {factor}")
                data[factor][detector] = str(path)
        made[pulsar] = {"par": str(par), "data": data}
    return made


@pytest.fixture(autouse=True)
def pe_data(files):
    """The parameter estimations know their data without the merge that provides it."""

    def data_files(self):
        factors = self.factors()
        return {
            f: {d: files[self.pulsar()]["data"][f][d] for d in self.interferometers()}
            for f in factors
        }

    with patch.object(PEPipeline, "data_files", data_files):
        yield


def blueprint(files, pulsars=PULSARS, extra="", pe_extra=None, detectors="[H1, L1]"):
    text = "kind: event\nname: O4a-heterodyne\ninterferometers: [H1, L1]\n"
    for pulsar in pulsars:
        factors = (pe_extra or {}).get(pulsar, "frequency factors: [2]\n")
        text += (
            f"---\nkind: event\nname: {pulsar}\nephemeris: {files[pulsar]['par']}\n"
            f"interferometers: {detectors}\n{factors}"
            f"---\nkind: analysis\nname: pe\npipeline: cwinpy-pe\nevent: {pulsar}\n"
        )
    needs = ", ".join(f"{p}/pe" for p in pulsars)
    text += (
        "---\nkind: analysis\nname: summary\npipeline: cwinpy-summary\nevent: O4a-heterodyne\n"
        f"needs: [{needs}]\n{extra}"
    )
    return text


def finish(project, pulsars=PULSARS):
    """Give each parameter estimation the result that it writes when it is done."""
    for pulsar in pulsars:
        pipeline = project.analysis(pulsar, "pe").pipeline
        os.makedirs(os.path.dirname(pipeline.result_file()), exist_ok=True)
        with open(pipeline.result_file(), "w") as handle:
            handle.write(f"result of {pulsar}")


def make(project, files, finished=True, **kwargs):
    project.apply(blueprint(files, **kwargs))
    if finished:
        finish(project, kwargs.get("pulsars", PULSARS))
    return project.analysis("O4a-heterodyne", "summary").pipeline


# What it summarises


def test_each_parameter_estimation_it_needs_is_a_run_with_what_cwinpy_reads(project, files):
    runs = make(project, files).runs()
    assert [r["pulsar"] for r in runs] == list(PULSARS)
    first = runs[0]
    assert first["ephemeris"] == files["J0534+2200"]["par"]
    assert first["detectors"] == ["H1", "L1"] and first["factors"] == [2]
    assert first["data"] == {2: files["J0534+2200"]["data"][2]}
    assert first["result"].endswith(os.path.join("J0534+2200", "pe", "results", "pe_result.hdf5"))


def test_the_runs_are_in_the_order_of_the_pulsars_whatever_the_order_of_the_needs(project, files):
    runs = make(project, files, pulsars=tuple(reversed(PULSARS))).runs()
    assert [r["pulsar"] for r in runs] == sorted(PULSARS)


def test_the_runs_are_sorted_even_if_asimov_gives_the_dependencies_in_another_order(project, files):
    pipeline = make(project, files)
    dependencies = list(pipeline.production.dependency_objects)
    assert len(dependencies) == 2
    with patch.object(
        type(pipeline.production),
        "dependency_objects",
        new_callable=PropertyMock,
        return_value=list(reversed(dependencies)),
    ):
        assert [r["pulsar"] for r in pipeline.runs()] == list(PULSARS)


def test_two_parameter_estimations_of_one_pulsar_are_refused_because_cwinpy_wants_one(
    project, files
):
    text = blueprint(files).replace(
        "---\nkind: analysis\nname: summary",
        "---\nkind: analysis\nname: pe-again\npipeline: cwinpy-pe\nevent: J0534+2200\n"
        "---\nkind: analysis\nname: summary",
    )
    text = text.replace("needs: [J0534+2200/pe,", "needs: [J0534+2200/pe, J0534+2200/pe-again,")
    project.apply(text)
    finish(project)
    again = project.analysis("J0534+2200", "pe-again").pipeline
    os.makedirs(os.path.dirname(again.result_file()), exist_ok=True)
    open(again.result_file(), "w").write("another")
    pipeline = project.analysis("O4a-heterodyne", "summary").pipeline
    with pytest.raises(PipelineException, match="More than one parameter estimation of J0534"):
        pipeline.runs()


def test_the_detectors_are_sorted_for_the_name_of_the_result(project, files):
    runs = make(project, files, detectors="[L1, H1]").runs()
    assert runs[0]["detectors"] == ["H1", "L1"]


def test_a_parameter_estimation_that_is_not_complete_is_an_error_that_names_it(project, files):
    project.apply(blueprint(files))
    finish(project, ["J0534+2200"])
    pipeline = project.analysis("O4a-heterodyne", "summary").pipeline
    with pytest.raises(
        PipelineException, match="parameter estimation of J0537-6910 is not complete"
    ):
        pipeline.runs()


def test_one_that_was_given_up_on_is_left_out(project, files):
    project.apply(blueprint(files))
    finish(project, ["J0534+2200"])
    project.set_status("J0537-6910", "pe", "skipped")
    pipeline = project.analysis("O4a-heterodyne", "summary").pipeline
    assert [r["pulsar"] for r in pipeline.runs()] == ["J0534+2200"]


def test_simulated_data_has_nothing_to_show(project, files):
    extra = {"J0537-6910": "frequency factors: [2]\n"}
    project.apply(blueprint(files, pe_extra=extra))
    finish(project)
    ledger = project.open()
    event = ledger.get_event("J0537-6910")[0]
    [a for a in event.productions if a.name == "pe"][0].meta["cwinpy-pe"] = {
        "fake asd": {"H1": 1e-24, "L1": 1e-24}
    }
    ledger.update_event(event)
    ledger.save()
    pipeline = project.analysis("O4a-heterodyne", "summary").pipeline
    with pytest.raises(PipelineException, match="J0537-6910 analysed simulated data"):
        pipeline.runs()


def test_there_is_nothing_to_summarise_without_a_parameter_estimation(project, files):
    project.apply(
        "kind: event\nname: O4a-heterodyne\ninterferometers: [H1, L1]\n"
        "---\nkind: analysis\nname: summary\npipeline: cwinpy-summary\nevent: O4a-heterodyne\n"
    )
    with pytest.raises(PipelineException, match="nothing to summarise"):
        project.analysis("O4a-heterodyne", "summary").pipeline.runs()


def test_pulsars_with_different_detectors_are_refused_because_cwinpy_takes_one_set(project, files):
    project.apply(blueprint(files))
    finish(project)
    ledger = project.open()
    event = ledger.get_event("J0537-6910")[0]
    event.meta["interferometers"] = ["H1"]
    [a for a in event.productions if a.name == "pe"][0].meta["interferometers"] = ["H1"]
    ledger.update_event(event)
    ledger.save()
    pipeline = project.analysis("O4a-heterodyne", "summary").pipeline
    with pytest.raises(PipelineException, match="differ in their detectors"):
        pipeline.runs()


def test_pulsars_with_different_factors_are_refused(project, files):
    extra = {"J0534+2200": "frequency factors: [2]\n", "J0537-6910": "frequency factors: [1, 2]\n"}
    pipeline = make(project, files, pe_extra=extra)
    with pytest.raises(PipelineException, match="differ in their factors"):
        pipeline.runs()


def test_the_check_before_the_configuration_runs_the_same_checks(project, files):
    project.apply(blueprint(files))
    finish(project, ["J0534+2200"])
    pipeline = project.analysis("O4a-heterodyne", "summary").pipeline
    with pytest.raises(PipelineException, match="J0537-6910 is not complete"):
        pipeline.before_config()


def test_the_check_before_the_configuration_refuses_a_setting_that_cwinpy_does_not_have(
    project, files
):
    pipeline = make(project, files, extra="cwinpy-summary:\n  color: blue\n")
    with pytest.raises(PipelineException, match="no setting color"):
        pipeline.before_config()


def test_the_check_passes_when_everything_is_complete_and_captures_the_environment(project, files):
    make(project, files).before_config()
    SummaryPipeline._capture_environment.assert_called_once()


# The settings


def test_the_settings_are_the_options_of_cwinpys_generator(project, files):
    extra = (
        "cwinpy-summary:\n  time series: false\n  upper limit plot: false\n  sort by: F0\n"
        "  sort descending: true\n  only msps: true\n  snr: true\n  odds: true\n"
        "  only joint: true\n  posteriors: false\n  individual posteriors: true\n"
        "  odds plot: false\n"
    )
    assert make(project, files, extra=extra).options() == {
        "showtimeseries": False,
        "upperlimitplot": False,
        "sortby": "F0",
        "sortdescending": True,
        "onlymsps": True,
        "showsnr": True,
        "showodds": True,
        "onlyjoint": True,
        "showposteriors": False,
        "showindividualposteriors": True,
        "oddsplot": False,
    }


def test_what_is_not_set_is_left_to_cwinpy(project, files):
    assert make(project, files).options() == {}


# The configuration, and where things are staged


def test_the_staged_files_are_in_the_run_directory_by_the_names_cwinpy_looks_for(project, files):
    pipeline = make(project, files)
    run = pipeline.runs()[0]
    rundir = pipeline.production.rundir
    staged = pipeline.staged(run)
    assert staged["par"] == os.path.join(rundir, "par", "J0534+2200.par")
    assert staged["data"][2]["H1"] == os.path.join(rundir, "data", "H1_2f_J0534+2200.hdf5")
    assert staged["result"] == os.path.join(
        rundir, "results", "J0534+2200", "cwinpy_pe_H1L1_J0534+2200_result.hdf5"
    )


def parsed(pipeline):
    parser = configparser.ConfigParser(interpolation=None)
    parser.read_string(pipeline.render_config())
    return parser


def test_the_configuration_points_at_what_is_staged(project, files):
    pipeline = make(project, files)
    config = parsed(pipeline)
    rundir = pipeline.production.rundir
    assert config["run"]["basedir"] == rundir
    assert config["pe_dag"]["build"] == "False"  # nothing is built or submitted
    pulsars = ast.literal_eval(config["ephemerides"]["pulsarfiles"])
    assert pulsars == {p: os.path.join(rundir, "par", f"{p}.par") for p in PULSARS}
    data = ast.literal_eval(config["pe"]["data-file-2f"])
    assert sorted(data) == ["H1", "L1"]
    assert data["L1"]["J0537-6910"] == os.path.join(rundir, "data", "L1_2f_J0537-6910.hdf5")
    assert "data-file-1f" not in config["pe"]


def test_each_factor_the_pulsars_were_analysed_at_has_its_own_option(project, files):
    both = {p: "frequency factors: [1, 2]\n" for p in PULSARS}
    config = parsed(make(project, files, pe_extra=both))
    assert "data-file-1f" in config["pe"] and "data-file-2f" in config["pe"]


def test_asimov_makes_the_configuration_file_from_the_template(project, files, tmp_path):
    pipeline = make(project, files)
    target = tmp_path / "summary.ini"
    pipeline.production.make_config(str(target))
    assert target.read_text().strip() == pipeline.render_config().strip()


# Building and submitting


def configured(project, files, tmp_path, **kwargs):
    pipeline = make(project, files, **kwargs)
    config = tmp_path / "made.ini"
    config.write_text(pipeline.render_config())
    repository = MagicMock(directory=str(tmp_path))
    repository.find_prods.return_value = [config.name]
    pipeline.production.event.repository = repository
    pipeline.category = ""
    return pipeline


def test_building_stages_what_cwinpy_reads(project, files, tmp_path):
    pipeline = configured(project, files, tmp_path)
    assert pipeline.build_dag() == pipeline.plan_file()
    for run in pipeline.runs():
        staged = pipeline.staged(run)
        assert open(staged["par"]).read() == open(run["ephemeris"]).read()
        for factor, by_detector in run["data"].items():
            for detector, source in by_detector.items():
                assert open(staged["data"][factor][detector]).read() == open(source).read()
        assert os.path.realpath(staged["result"]) == os.path.realpath(run["result"])


def test_the_inputs_are_not_symbolic_links_which_cwinpy_refuses_but_the_results_are(
    project, files, tmp_path
):
    pipeline = configured(project, files, tmp_path)
    pipeline.build_dag()
    staged = pipeline.staged(pipeline.runs()[0])
    assert not os.path.islink(staged["par"])
    assert not os.path.islink(staged["data"][2]["H1"])
    assert os.path.islink(staged["result"])


def test_the_inputs_are_hard_links_where_the_file_system_allows(project, files, tmp_path):
    pipeline = configured(project, files, tmp_path)
    pipeline.build_dag()
    run = pipeline.runs()[0]
    assert os.path.samefile(pipeline.staged(run)["par"], run["ephemeris"])


def test_they_are_copies_where_it_does_not(project, files, tmp_path):
    pipeline = configured(project, files, tmp_path)
    with patch("os.link", side_effect=OSError("cross-device link")):
        pipeline.build_dag()
    run = pipeline.runs()[0]
    staged = pipeline.staged(run)["par"]
    assert not os.path.samefile(staged, run["ephemeris"])
    assert open(staged).read() == open(run["ephemeris"]).read()


def test_the_plan_says_where_the_job_reads_and_writes(project, files, tmp_path):
    pipeline = configured(project, files, tmp_path, extra="cwinpy-summary:\n  time series: false\n")
    pipeline.build_dag()
    plan = json.load(open(pipeline.plan_file()))
    rundir = pipeline.production.rundir
    assert plan["status"] == os.path.join(rundir, "summary-status.json")
    assert plan["config"] == os.path.join(rundir, "summary.ini")
    assert open(plan["config"]).read() == open(tmp_path / "made.ini").read()
    assert plan["outpath"] == os.path.join(rundir, "pages")
    assert plan["url"] == "O4a-heterodyne/summary"
    assert plan["options"] == {"showtimeseries": False}
    staged = [pipeline.staged(r) for r in pipeline.runs()]
    expected = {s["par"] for s in staged}
    expected |= {path for s in staged for by in s["data"].values() for path in by.values()}
    expected |= {r["result"] for r in pipeline.runs()}
    assert set(plan["inputs"]) == expected


def test_building_again_leaves_the_same_staging(project, files, tmp_path):
    pipeline = configured(project, files, tmp_path)
    pipeline.build_dag()
    pipeline.build_dag()
    staged = pipeline.staged(pipeline.runs()[0])
    assert os.path.exists(staged["par"]) and os.path.islink(staged["result"])


def test_a_dry_run_stages_nothing(project, files, tmp_path):
    pipeline = configured(project, files, tmp_path)
    pipeline.build_dag(dryrun=True)
    assert not os.path.exists(pipeline.plan_file())
    assert not os.path.exists(os.path.join(pipeline.production.rundir, "par"))


def test_building_without_a_configuration_is_an_error(project, files, tmp_path):
    pipeline = configured(project, files, tmp_path)
    pipeline.production.event.repository.find_prods.return_value = []
    with pytest.raises(PipelineException, match="No configuration file found"):
        pipeline.build_dag()


def test_building_with_a_parameter_estimation_that_is_not_complete_is_an_error(
    project, files, tmp_path
):
    pipeline = configured(project, files, tmp_path)
    os.remove(pipeline.runs()[0]["result"])
    with pytest.raises(PipelineException, match="not complete"):
        pipeline.build_dag()


def test_the_job_is_the_summary_job_with_the_plan(project, files, tmp_path):
    pipeline = configured(project, files, tmp_path)
    scheduler = MagicMock()
    scheduler.submit.return_value = 88
    with patch.object(
        SummaryPipeline, "scheduler", new_callable=PropertyMock, return_value=scheduler
    ):
        assert pipeline.submit_dag() == 88
    job = scheduler.submit.call_args[0][0]
    assert job.kwargs["arguments"] == f"-m asimov_cwinpy.summary_job {pipeline.plan_file()}"
    assert job.kwargs["batch_name"] == "cwinpy-summary/O4a-heterodyne/summary"
    assert pipeline.production.job_id == 88


# Knowing how it went


def say(pipeline, **status):
    os.makedirs(pipeline.production.rundir, exist_ok=True)
    with open(pipeline.status_file(), "w") as handle:
        json.dump(status, handle)


def pages(pipeline):
    os.makedirs(os.path.dirname(pipeline.home()), exist_ok=True)
    open(pipeline.home(), "w").write("<html>")


def test_it_is_complete_when_the_job_says_so_and_the_home_page_is_there(project, files):
    pipeline = make(project, files)
    assert not pipeline.detect_completion()
    say(pipeline, status="succeeded")
    assert not pipeline.detect_completion()  # no page
    pages(pipeline)
    assert pipeline.detect_completion()
    os.remove(pipeline.status_file())
    assert not pipeline.detect_completion()  # a page left by a run that said nothing


def test_a_job_that_said_it_failed_is_an_error_that_is_transient_only_if_it_says_so(project, files):
    pipeline = make(project, files)
    say(pipeline, status="failed", error="ValueError: x", transient=False)
    with pytest.raises(CwinpyError, match="ValueError: x") as error:
        pipeline.resurrect()
    assert not isinstance(error.value, TransientCwinpyError)
    say(pipeline, status="failed", error="OSError: x", transient=True)
    with pytest.raises(TransientCwinpyError):
        pipeline.resurrect()


def test_a_job_that_said_it_succeeded_without_making_the_page_is_an_error(project, files):
    pipeline = make(project, files)
    say(pipeline, status="succeeded")
    with pytest.raises(CwinpyError, match="succeeded but .* not there"):
        pipeline.resurrect()


def test_a_job_that_has_said_nothing_is_left_alone(project, files):
    make(project, files).resurrect()


def test_the_asset_is_the_home_page_once_it_is_complete(project, files):
    pipeline = make(project, files)
    assert pipeline.collect_assets() == {}
    say(pipeline, status="succeeded")
    pages(pipeline)
    assert pipeline.collect_assets() == {"summary pages": pipeline.home()}
    assert pipeline.output_paths() == {"summary pages": [pipeline.pages_directory()]}


# In the report


@pytest.fixture
def webroot(tmp_path):
    root = tmp_path / "webroot"
    with patch.object(SummaryPipeline, "published", lambda self: str(root / "O4a" / "summary")):
        yield root


def test_when_it_finishes_the_pages_are_copied_to_the_web_directory(project, files, webroot):
    pipeline = make(project, files)
    pages(pipeline)
    os.makedirs(os.path.join(pipeline.pages_directory(), "css"))
    open(os.path.join(pipeline.pages_directory(), "css", "table.css"), "w").write("x")
    pipeline.after_completion()
    assert (webroot / "O4a" / "summary" / "html" / "home.html").exists()
    assert (webroot / "O4a" / "summary" / "css" / "table.css").exists()
    assert str(pipeline.production.status).lower() == "finished"


def test_finishing_again_copies_the_pages_over_the_old_ones(project, files, webroot):
    pipeline = make(project, files)
    pages(pipeline)
    pipeline.after_completion()
    open(pipeline.home(), "w").write("<html>newer")
    pipeline.after_completion()
    assert (webroot / "O4a" / "summary" / "html" / "home.html").read_text() == "<html>newer"


def test_a_copy_that_fails_does_not_stop_it_finishing(project, files, webroot):
    pipeline = make(project, files)
    pages(pipeline)
    with patch("shutil.copytree", side_effect=OSError("disk full")):
        pipeline.after_completion()
    assert str(pipeline.production.status).lower() == "finished"
    assert pipeline.result_pages() == []


def test_without_a_web_directory_it_still_finishes(project, files):
    pipeline = make(project, files)
    pages(pipeline)
    with patch.object(SummaryPipeline, "published", lambda self: None):
        pipeline.after_completion()
        assert pipeline.result_pages() == []
    assert str(pipeline.production.status).lower() == "finished"


def test_the_report_links_the_home_page_relative_to_the_report(project, files, webroot):
    pipeline = make(project, files)
    assert pipeline.result_pages() == []  # not there yet
    pages(pipeline)
    pipeline.after_completion()
    assert pipeline.result_pages() == [
        ("cwinpy summary pages", "O4a-heterodyne/summary/html/home.html")
    ]


def test_the_web_directory_is_that_of_the_project(project, files, tmp_path):
    pipeline = make(project, files)
    with patch.object(summary.config, "get", return_value=str(tmp_path / "web")):
        assert pipeline.published() == str(tmp_path / "web" / "O4a-heterodyne" / "summary")
    with patch.object(summary.config, "get", side_effect=KeyError("webroot")):
        assert pipeline.published() is None


def test_the_report_of_asimov_shows_the_link_for_a_finished_summary(project, files, tmp_path):
    """The report modal of the event reads `result_pages` of each finished analysis."""
    pipeline = make(project, files)
    pages(pipeline)
    root = tmp_path / "web"
    with patch.object(
        SummaryPipeline, "published", lambda self: str(root / "O4a-heterodyne" / "summary")
    ):
        pipeline.after_completion()
        project.set_status("O4a-heterodyne", "summary", "finished")
        event = project.open().get_event("O4a-heterodyne")[0]
        html = event.html()
    assert "O4a-heterodyne/summary/html/home.html|cwinpy summary pages" in html
