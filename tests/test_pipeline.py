"""Tests for the example pipeline implementation."""

import os
from unittest.mock import MagicMock, Mock, patch

import pytest
from asimov.pipeline import PipelineException
from asimov.scheduler import Slurm

from asimov_myplugin import MyPlugin


@pytest.fixture
def mock_production():
    """Create a mock production object for testing."""
    production = MagicMock()
    production.name = "TestProduction"
    production.pipeline = "myplugin"
    production.category = "analyses"
    production.rundir = "/tmp/test_rundir"
    production.status = "wait"
    production.job_id = None
    production.priors = None

    production.event = MagicMock()
    production.event.name = "GW150914"
    production.event.repository = MagicMock()
    production.event.repository.directory = "/tmp/test_repo"
    production.event.repository.find_prods.return_value = ["TestProduction.ini"]

    production.meta = {
        "event time": 1126259462.4,
        "interferometers": ["H1", "L1"],
        "waveform": {"approximant": "IMRPhenomXPHM"},
        "likelihood": {"sample rate": 4096, "minimum frequency": {"H1": 20, "L1": 25}},
        "scheduler": {"accounting group": "my.accounting.group", "request cpus": 2},
    }

    return production


@pytest.fixture
def mock_config(monkeypatch):
    """Mock the asimov config object."""
    config_values = {
        ("pipelines", "environment"): "/opt/conda",
        ("condor", "user"): "test.user",
    }

    def mock_get(section, key):
        return config_values.get((section, key), "")

    mock_config_module = MagicMock()
    mock_config_module.get = mock_get

    monkeypatch.setattr("asimov_myplugin.pipeline.config", mock_config_module)
    return mock_config_module


@pytest.fixture
def temp_dir(tmp_path):
    return str(tmp_path)


class TestInit:
    def test_init_success(self, mock_production, mock_config):
        pipeline = MyPlugin(mock_production)
        assert pipeline.name == "My Plugin"
        assert pipeline.production == mock_production

    def test_init_wrong_pipeline(self, mock_production, mock_config):
        mock_production.pipeline = "someotherpipeline"
        with pytest.raises(PipelineException, match="Pipeline mismatch"):
            MyPlugin(mock_production)


class TestConfigTemplate:
    def test_config_template_is_a_real_bundled_file(self, mock_production, mock_config):
        pipeline = MyPlugin(mock_production)
        assert os.path.exists(pipeline.config_template)

    def test_config_template_is_named_myplugin_ini(self, mock_production, mock_config):
        pipeline = MyPlugin(mock_production)
        assert os.path.basename(pipeline.config_template) == "myplugin.ini"


class TestConfigRendering:
    """Render the bundled template the way ``Analysis.make_config`` does."""

    def render(self, production):
        from liquid import Liquid

        pipeline = MyPlugin(production)
        template = Liquid(pipeline.config_template, from_file=True)
        return template.render(production=production, analysis=production, pipeline=pipeline)

    def test_template_renders_standard_ledger_values(self, mock_production, mock_config):
        mock_production.priors = {
            "chirp mass": {"type": "uniform", "minimum": 20.0, "maximum": 40.0},
        }
        rendered = self.render(mock_production)
        assert "instruments = H1 L1" in rendered
        assert "sample-rate = 4096" in rendered
        assert "approximant = IMRPhenomXPHM" in rendered
        assert "f-low = 20.0" in rendered
        assert "chirp mass = uniform(20.0, 40.0)" in rendered
        assert "warmup = 500" in rendered

    def test_template_reads_namespaced_setting(self, mock_production, mock_config):
        mock_production.meta["myplugin"] = {"warmup steps": 1000}
        assert "warmup = 1000" in self.render(mock_production)

    def test_rendering_does_not_write_to_the_ledger(self, mock_production, mock_config):
        mock_production.priors = {
            "chirp mass": {"type": "uniform", "minimum": 20.0, "maximum": 40.0},
        }
        before = dict(mock_production.meta)
        pipeline = MyPlugin(mock_production)
        pipeline.before_config()
        self.render(mock_production)
        assert mock_production.meta == before


class TestBeforeConfig:
    def test_before_config_accepts_complete_ledger(self, mock_production, mock_config):
        MyPlugin(mock_production).before_config()

    @pytest.mark.parametrize(
        "section, key",
        [
            ("likelihood", "sample rate"),
            ("likelihood", "minimum frequency"),
            ("waveform", "approximant"),
        ],
    )
    def test_before_config_raises_on_missing_scientific_input(
        self, mock_production, mock_config, section, key
    ):
        del mock_production.meta[section][key]
        pipeline = MyPlugin(mock_production)
        with pytest.raises(PipelineException, match=key):
            pipeline.before_config()


class TestDerivedValues:
    def test_minimum_frequency_uses_lowest_detector_value(self, mock_production, mock_config):
        assert MyPlugin(mock_production).minimum_frequency() == 20.0

    def test_minimum_frequency_raises_for_missing_detector(self, mock_production, mock_config):
        mock_production.meta["interferometers"] = ["H1", "L1", "V1"]
        with pytest.raises(PipelineException, match="V1"):
            MyPlugin(mock_production).minimum_frequency()

    def test_render_priors_is_empty_without_priors(self, mock_production, mock_config):
        assert MyPlugin(mock_production).render_priors() == ""

    def test_render_priors_formats_each_prior(self, mock_production, mock_config):
        mock_production.priors = {
            "mass": {"type": "uniform", "minimum": 10.0, "maximum": 80.0},
            "psi": {"type": "gaussian", "mu": 0.0, "sigma": 1.0},
        }
        rendered = MyPlugin(mock_production).render_priors()
        assert rendered.splitlines() == ["mass = uniform(10.0, 80.0)", "psi = gaussian(0.0, 1.0)"]

    def test_render_priors_raises_for_unsupported_type(self, mock_production, mock_config):
        # No plugin-specific escape hatch: an unsupported prior is an error.
        mock_production.priors = {"a_1": {"type": "power_law"}}
        with pytest.raises(PipelineException, match="power_law"):
            MyPlugin(mock_production).render_priors()


class TestBuildDag:
    def test_build_dag_resolves_rundir(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = os.path.join(temp_dir, "run")
        pipeline = MyPlugin(mock_production)
        config_file = pipeline.build_dag()
        assert os.path.isdir(mock_production.rundir)
        assert config_file.endswith("TestProduction.ini")

    def test_build_dag_raises_without_rundir(self, mock_production, mock_config):
        mock_production.rundir = None
        pipeline = MyPlugin(mock_production)
        with pytest.raises(PipelineException, match="working directory"):
            pipeline.build_dag()

    def test_build_dag_dryrun_does_not_create_rundir(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = os.path.join(temp_dir, "run")
        MyPlugin(mock_production).build_dag(dryrun=True)
        assert not os.path.exists(mock_production.rundir)

    def test_build_dag_raises_if_no_config_found(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = os.path.join(temp_dir, "run")
        mock_production.event.repository.find_prods.return_value = []
        pipeline = MyPlugin(mock_production)
        with pytest.raises(PipelineException, match="No configuration file found"):
            pipeline.build_dag()

    def test_build_dag_dryrun_does_not_raise(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = os.path.join(temp_dir, "run")
        pipeline = MyPlugin(mock_production)
        pipeline.build_dag(dryrun=True)

    def test_build_dag_without_event_repository(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = os.path.join(temp_dir, "run")
        mock_production.event.repository = None
        pipeline = MyPlugin(mock_production)
        assert pipeline.build_dag() == "TestProduction.ini"


class TestSubmitDag:
    def test_submit_dag_uses_scheduler_abstraction(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = os.path.join(temp_dir, "run")
        with patch(
            "asimov_myplugin.pipeline.shutil.which", return_value="/opt/conda/bin/my-executable"
        ):
            pipeline = MyPlugin(mock_production)
            pipeline._scheduler = Mock()
            pipeline._scheduler.submit.return_value = 12345

            cluster_id = pipeline.submit_dag(dryrun=False)

        assert cluster_id == 12345
        assert mock_production.job_id == 12345
        assert mock_production.status == "running"

    def test_submit_dag_requests_gpu_on_htcondor(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = os.path.join(temp_dir, "run")
        mock_production.meta["scheduler"]["request gpus"] = 1
        with patch(
            "asimov_myplugin.pipeline.shutil.which", return_value="/opt/conda/bin/my-executable"
        ):
            pipeline = MyPlugin(mock_production)
            pipeline._scheduler = Mock()  # not a Slurm instance
            pipeline._scheduler.submit.return_value = 1

            pipeline.submit_dag(dryrun=False)

        job = pipeline._scheduler.submit.call_args[0][0]
        assert job.kwargs["request_gpus"] == "1"
        assert "slurm_gres" not in job.kwargs

    def test_submit_dag_requests_gpu_on_slurm(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = os.path.join(temp_dir, "run")
        mock_production.meta["scheduler"]["request gpus"] = 1
        with patch(
            "asimov_myplugin.pipeline.shutil.which", return_value="/opt/conda/bin/my-executable"
        ):
            pipeline = MyPlugin(mock_production)
            pipeline._scheduler = Mock(spec=Slurm)
            pipeline._scheduler.submit.return_value = 1

            pipeline.submit_dag(dryrun=False)

        job = pipeline._scheduler.submit.call_args[0][0]
        assert job.kwargs["slurm_gres"] == "gpu:1"
        assert "request_gpus" not in job.kwargs

    def test_submit_dag_dryrun_does_not_call_scheduler(
        self, mock_production, mock_config, temp_dir
    ):
        mock_production.rundir = os.path.join(temp_dir, "run")
        with patch(
            "asimov_myplugin.pipeline.shutil.which", return_value="/opt/conda/bin/my-executable"
        ):
            pipeline = MyPlugin(mock_production)
            pipeline._scheduler = Mock()

            result = pipeline.submit_dag(dryrun=True)

        assert result is None
        pipeline._scheduler.submit.assert_not_called()

    def test_submit_dag_missing_executable_raises_clear_exception(
        self, mock_production, mock_config, temp_dir
    ):
        mock_production.rundir = os.path.join(temp_dir, "run")
        with patch("asimov_myplugin.pipeline.shutil.which", return_value=None):
            pipeline = MyPlugin(mock_production)
            with pytest.raises(PipelineException, match="my-executable"):
                pipeline.submit_dag(dryrun=False)

    def test_submit_dag_scheduler_failure(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = os.path.join(temp_dir, "run")
        with patch(
            "asimov_myplugin.pipeline.shutil.which", return_value="/opt/conda/bin/my-executable"
        ):
            pipeline = MyPlugin(mock_production)
            pipeline._scheduler = Mock()
            pipeline._scheduler.submit.side_effect = RuntimeError("could not submit")

            with pytest.raises(PipelineException, match="could not be submitted"):
                pipeline.submit_dag(dryrun=False)

    def test_submit_dag_sets_accounting_group(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = os.path.join(temp_dir, "run")
        with patch(
            "asimov_myplugin.pipeline.shutil.which", return_value="/opt/conda/bin/my-executable"
        ):
            pipeline = MyPlugin(mock_production)
            pipeline._scheduler = Mock()
            pipeline._scheduler.submit.return_value = 1

            pipeline.submit_dag(dryrun=False)

        job = pipeline._scheduler.submit.call_args[0][0]
        assert job.kwargs["accounting_group"] == "my.accounting.group"
        assert job.kwargs["cpus"] == "2"


class TestDetectCompletion:
    def test_detect_completion_no_output(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = temp_dir
        pipeline = MyPlugin(mock_production)
        assert pipeline.detect_completion() is False

    def test_detect_completion_marker_present(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = temp_dir
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir)
        with open(os.path.join(output_dir, "result.dat"), "w") as f:
            f.write("x")
        pipeline = MyPlugin(mock_production)
        assert pipeline.detect_completion() is True


class TestSamplesAndAssets:
    def test_samples_empty_when_no_output(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = temp_dir
        pipeline = MyPlugin(mock_production)
        assert pipeline.samples() == []

    def test_samples_returns_samples_file(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = temp_dir
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir)
        samples_path = os.path.join(output_dir, "samples.dat")
        with open(samples_path, "w") as f:
            f.write("x")
        pipeline = MyPlugin(mock_production)
        assert pipeline.samples() == [samples_path]

    def test_collect_assets_includes_config(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = temp_dir
        pipeline = MyPlugin(mock_production)
        assets = pipeline.collect_assets()
        assert assets["config"] == "TestProduction.ini"
        assert assets["samples"] == []


class TestCollectLogs:
    def test_collect_logs_reads_log_files(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = temp_dir
        with open(os.path.join(temp_dir, "TestProduction.out"), "w") as f:
            f.write("stdout contents")
        pipeline = MyPlugin(mock_production)
        logs = pipeline.collect_logs()
        assert logs["TestProduction.out"] == "stdout contents"

    def test_collect_logs_empty_when_no_logs(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = temp_dir
        pipeline = MyPlugin(mock_production)
        assert pipeline.collect_logs() == {}


class TestAfterCompletion:
    def test_after_completion_marks_production_finished(
        self, mock_production, mock_config, temp_dir
    ):
        mock_production.rundir = temp_dir
        mock_production.status = "running"
        pipeline = MyPlugin(mock_production)
        pipeline.after_completion()
        assert mock_production.status == "finished"


class TestResurrect:
    def test_resurrect_resubmits_job(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = temp_dir
        with patch(
            "asimov_myplugin.pipeline.shutil.which", return_value="/opt/conda/bin/my-executable"
        ):
            pipeline = MyPlugin(mock_production)
            pipeline._scheduler = Mock()
            pipeline._scheduler.submit.return_value = 99

            pipeline.resurrect()

        assert pipeline._scheduler.submit.called
        with open(os.path.join(temp_dir, ".resurrections")) as handle:
            assert handle.read() == "1"
        assert "resurrections" not in mock_production.meta

    def test_resurrect_gives_up_after_five_attempts(self, mock_production, mock_config, temp_dir):
        mock_production.rundir = temp_dir
        with open(os.path.join(temp_dir, ".resurrections"), "w") as handle:
            handle.write("5")
        with patch(
            "asimov_myplugin.pipeline.shutil.which", return_value="/opt/conda/bin/my-executable"
        ):
            pipeline = MyPlugin(mock_production)
            pipeline._scheduler = Mock()

            pipeline.resurrect()

        pipeline._scheduler.submit.assert_not_called()


class TestGetPriorInterface:
    def test_returns_myplugin_prior_interface(self, mock_production, mock_config):
        from asimov_myplugin.priors import MyPluginPriorInterface

        pipeline = MyPlugin(mock_production)
        assert isinstance(pipeline.get_prior_interface(), MyPluginPriorInterface)
