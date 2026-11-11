"""
The summary pipeline: ``pipeline: cwinpy-summary``.

cwinpy makes pages of results for a set of pulsars (``cwinpy_generate_summary_pages``): a table of
upper limits, and for each pulsar its posteriors and its data. They are made for the whole set from
the configuration of cwinpy's own pipeline, which finds its results and data by the names and the
directories that it gives them. The ``cwinpy_pe`` jobs of this plugin are separate, and write
where asimov says, so this analysis, which needs every parameter estimation of a run, *stages* what
cwinpy looks for:

``par/<pulsar>.par``, ``data/<detector>_<factor>f_<pulsar>.hdf5``
    The ephemeris and the merged heterodyned data of each pulsar, as hard links (or copies if the
    file system does not allow it), because cwinpy refuses a link that is symbolic.
``results/<pulsar>/cwinpy_pe_<detectors>_<pulsar>_result.hdf5``
    The result of each parameter estimation, which is a link to the file that it wrote.

and writes the configuration that points at them. One job then makes the pages, and when it has
succeeded they are copied to the web directory of the project, where the report of asimov links
them (``Pipeline.result_pages``).
"""

import configparser
import importlib.resources
import io
import json
import os
import shutil

from asimov import config
from asimov.context import resolve_path
from asimov.pipeline import PipelineException

from .common import CwinpyError, TransientCwinpyError
from .pe import PEPipeline
from .singlejob import SingleJobPipeline

#: The asset: the page that the others are reached from, once the pages are made.
ASSET = "summary pages"

#: What the job writes to say how it went, and what it reads, in the run directory.
STATUS = "summary-status.json"
PLAN = "summary-plan.json"

#: The configuration that cwinpy reads, in the run directory.
CONFIG = "summary.ini"

#: The page that the others are reached from, relative to the directory of the pages.
HOME = os.path.join("html", "home.html")

#: The settings of ``cwinpy-summary:``, and the option of ``generate_summary_pages`` that each is.
OPTIONS = {
    "posteriors": "showposteriors",
    "individual posteriors": "showindividualposteriors",
    "time series": "showtimeseries",
    "upper limit plot": "upperlimitplot",
    "odds plot": "oddsplot",
    "snr": "showsnr",
    "odds": "showodds",
    "sort by": "sortby",
    "sort descending": "sortdescending",
    "only joint": "onlyjoint",
    "only msps": "onlymsps",
}


def link_or_copy(source, target):
    """
    Put a file where cwinpy will look for it, as a hard link, or a copy where that is not possible.

    cwinpy refuses a symbolic link as an input file, because HTCondor would not transfer it.
    """
    os.makedirs(os.path.dirname(target), exist_ok=True)
    if os.path.lexists(target):
        os.remove(target)
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)


def symlink(source, target):
    """Make a symbolic link, replacing what is there."""
    os.makedirs(os.path.dirname(target), exist_ok=True)
    if os.path.lexists(target):
        os.remove(target)
    os.symlink(source, target)


class SummaryPipeline(SingleJobPipeline):
    """Make the summary pages of the parameter estimations of a run."""

    name = "cwinpy-summary"
    pipeline_name = "cwinpy-summary"
    job_module = "asimov_cwinpy.summary_job"
    plan_name = PLAN
    status_name = STATUS

    available_outputs = [ASSET, "config"]
    declares_inputs = True

    # What it summarises

    def get_actual_inputs(self, production):
        return ["samples"]

    def settings(self):
        """The ``cwinpy-summary:`` block of the analysis."""
        return self.production.meta.get("cwinpy-summary") or {}

    def options(self):
        """
        The options for ``generate_summary_pages``, for the settings that are given.

        Raises
        ------
        PipelineException
            If a setting is not one that cwinpy has.
        """
        settings = self.settings()
        unknown = sorted(set(settings) - set(OPTIONS))
        if unknown:
            raise PipelineException(
                f"`cwinpy-summary` has no setting {', '.join(unknown)}; it has "
                f"{', '.join(sorted(OPTIONS))}.",
                production=self.production.name,
            )
        return {OPTIONS[key]: value for key, value in settings.items()}

    def runs(self):
        """
        The parameter estimations to summarise, with what cwinpy needs of each.

        Returns
        -------
        list of dict
            ``{pulsar, ephemeris, detectors, factors, data, result}`` for each parameter
            estimation it needs, by pulsar. ``data`` is ``{factor: {detector: path}}``. One that
            was given up on (``skipped``) is left out.

        Raises
        ------
        PipelineException
            If one is not complete, analysed simulated data (which has no heterodyned data to
            show), or differs from the others in detectors or frequency factors (cwinpy takes
            these for the whole set), or if there are none.
        """
        found = []
        for dependency in self.production.dependency_objects:
            pipe = dependency.pipeline
            if not isinstance(pipe, PEPipeline):
                continue
            if str(dependency.status).lower() == "skipped":
                self.logger.warning(
                    f"{dependency.event.name} was given up on; summarising without it"
                )
                continue
            pulsar = pipe.pulsar()
            if pipe.fake():
                raise PipelineException(
                    f"The parameter estimation of {pulsar} analysed simulated data, which has no "
                    f"heterodyned data to show: it cannot be in the summary pages.",
                    production=self.production.name,
                )
            if not pipe.detect_completion():
                raise PipelineException(
                    f"The parameter estimation of {pulsar} is not complete.",
                    production=self.production.name,
                )
            data = pipe.data_files()
            found.append(
                {
                    "pulsar": pulsar,
                    "ephemeris": dependency.meta["ephemeris"],
                    "detectors": sorted(pipe.interferometers()),
                    "factors": sorted(data),
                    "data": data,
                    "result": pipe.result_file(),
                }
            )
        if not found:
            raise PipelineException(
                f"{self.production.name} needs no cwinpy-pe analysis, so there is nothing to "
                f"summarise.",
                production=self.production.name,
            )
        found.sort(key=lambda run: run["pulsar"])
        names = [run["pulsar"] for run in found]
        repeated = sorted({name for name in names if names.count(name) > 1})
        if repeated:
            raise PipelineException(
                f"More than one parameter estimation of {', '.join(repeated)} is needed.",
                production=self.production.name,
            )
        for key in ("detectors", "factors"):
            if len({tuple(run[key]) for run in found}) > 1:
                raise PipelineException(
                    f"The pulsars differ in their {key}, and cwinpy makes the pages for one set "
                    f"of them. Summarise those that have the same in separate analyses.",
                    production=self.production.name,
                )
        return found

    # Where things are staged

    def pages_directory(self):
        """Where the job makes the pages."""
        return os.path.join(self.production.rundir, "pages")

    def staged(self, run):
        """
        Where the files of one pulsar are put for cwinpy.

        Returns
        -------
        dict
            ``par`` (a path), ``data`` (``{factor: {detector: path}}``) and ``result``.
        """
        rundir = self.production.rundir
        pulsar = run["pulsar"]
        return {
            "par": os.path.join(rundir, "par", f"{pulsar}.par"),
            "data": {
                factor: {
                    detector: os.path.join(rundir, "data", f"{detector}_{factor}f_{pulsar}.hdf5")
                    for detector in run["detectors"]
                }
                for factor in run["factors"]
            },
            "result": os.path.join(
                rundir,
                "results",
                pulsar,
                f"cwinpy_pe_{''.join(run['detectors'])}_{pulsar}_result.hdf5",
            ),
        }

    # The configuration

    def render_config(self):
        """
        The text of the configuration of cwinpy's pipeline, which says where to find what is staged.
        """
        runs = self.runs()
        parser = configparser.ConfigParser(interpolation=None)
        parser["run"] = {"basedir": self.production.rundir}
        parser["pe_dag"] = {"build": "False"}
        parser["ephemerides"] = {
            "pulsarfiles": repr({run["pulsar"]: self.staged(run)["par"] for run in runs})
        }
        parser["pe"] = {}
        for factor in runs[0]["factors"]:
            by_detector = {
                detector: {
                    run["pulsar"]: self.staged(run)["data"][factor][detector] for run in runs
                }
                for detector in runs[0]["detectors"]
            }
            parser["pe"][f"data-file-{factor}f"] = repr(by_detector)
        text = io.StringIO()
        parser.write(text)
        return text.getvalue().rstrip("\n")

    @property
    def config_template(self):
        """The bundled template that the configuration is made from."""
        return str(
            importlib.resources.files("asimov_cwinpy").joinpath("configs/cwinpy-summary.ini")
        )

    def before_config(self, dryrun=False):
        """
        Check that every parameter estimation it needs is complete, and the settings are known.

        Raises
        ------
        PipelineException
            If one is not complete, or none is needed, or a setting is not known.
        """
        self.runs()
        self.options()
        super().before_config(dryrun=dryrun)

    # Building and submitting

    def build_dag(self, dryrun=False):
        """
        Stage what cwinpy reads, and write the plan that the job reads.

        Returns
        -------
        str
            The path of the plan.
        """
        rundir = self.production.rundir
        if rundir is None:
            raise PipelineException(
                "This analysis has no run directory: set `working directory` on the subject "
                "(or `rundir` on the analysis) in the ledger.",
                production=self.production.name,
            )
        config_file = self._config_file()
        if dryrun:
            print(f"Configuration file: {config_file}")
            return self.plan_file()
        runs = self.runs()
        options = self.options()
        os.makedirs(rundir, exist_ok=True)
        inputs = []
        for run in runs:
            staged = self.staged(run)
            link_or_copy(run["ephemeris"], staged["par"])
            symlink(os.path.abspath(run["result"]), staged["result"])
            for factor, by_detector in run["data"].items():
                for detector, source in by_detector.items():
                    link_or_copy(source, staged["data"][factor][detector])
                    inputs.append(staged["data"][factor][detector])
            inputs.extend([staged["par"], run["result"]])
        shutil.copyfile(config_file, os.path.join(rundir, CONFIG))
        plan = {
            "status": self.status_file(),
            "config": os.path.join(rundir, CONFIG),
            "outpath": self.pages_directory(),
            "url": f"{self.production.event.name}/{self.production.name}",
            "options": options,
            "inputs": inputs,
        }
        with open(self.plan_file(), "w") as handle:
            json.dump(plan, handle, indent=1)
        return self.plan_file()

    # Knowing how it went

    def home(self):
        """The page of the summary that the others are reached from."""
        return os.path.join(self.pages_directory(), HOME)

    def detect_completion(self):
        """Whether the job said that it succeeded, and the home page is there."""
        if not self.production.rundir:
            return False
        status = self.job_status()
        return bool(status and status.get("status") == "succeeded") and os.path.exists(self.home())

    def resurrect(self):
        """
        The job has left the scheduler and the pages are not made.

        That is not always a failure, so this raises only when the job said that it failed, or
        said that it succeeded and the pages are not there. A job that has said nothing is left
        alone.

        Raises
        ------
        TransientCwinpyError
            If the job failed in a way that is likely not to happen again: a file system that was
            unwell.
        CwinpyError
            If it failed for another reason.
        """
        status = self.job_status()
        if status is None:
            self.logger.debug("The job has gone, but has said nothing; leaving it")
            return
        if status.get("status") == "failed":
            error = TransientCwinpyError if status.get("transient") else CwinpyError
            raise error(
                f"The summary job of {self.production.name} failed: {status.get('error')}. "
                f"See {self.production.name}.err in its run directory.",
                production=self.production.name,
            )
        if not os.path.exists(self.home()):
            raise CwinpyError(
                f"The summary job of {self.production.name} succeeded but {self.home()} is "
                f"not there.",
                production=self.production.name,
            )

    # Making them part of the report

    def published(self):
        """Where the pages are in the web directory of the project, or None if there is none."""
        try:
            webroot = resolve_path(config.get("general", "webroot"))
        except Exception:
            return None
        return os.path.join(webroot, self.production.event.name, self.production.name)

    def after_completion(self):
        """Copy the pages to the web directory, where the report links them, and finish."""
        destination = self.published()
        if destination is None:
            self.logger.warning("The project has no web directory, so the pages are not linked")
        else:
            try:
                shutil.copytree(self.pages_directory(), destination, dirs_exist_ok=True)
            except OSError as error:
                self.logger.warning(f"The pages could not be copied to {destination}: {error}")
        super().after_completion()

    def result_pages(self):
        """
        The page of the summary, for the report: ``(label, url)``, with the url relative to the
        report, once it is in the web directory.
        """
        destination = self.published()
        if destination is None or not os.path.exists(os.path.join(destination, HOME)):
            return []
        return [
            (
                "cwinpy summary pages",
                "/".join([self.production.event.name, self.production.name, *HOME.split(os.sep)]),
            )
        ]

    def collect_assets(self):
        """
        What the job has made.

        Returns
        -------
        dict
            ``summary pages``, the home page, once it is complete.
        """
        return {ASSET: self.home()} if self.detect_completion() else {}

    def output_paths(self):
        return {ASSET: [self.pages_directory()]}
