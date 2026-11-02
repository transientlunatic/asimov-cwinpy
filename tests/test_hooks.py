"""Tests for the example hook implementations."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from asimov_myplugin.hooks import MyApplicatorHook, MyPostMonitorHook


@pytest.fixture
def mock_ledger():
    """Create a mock ledger object for testing."""
    ledger = Mock()
    ledger.events = {}

    # Create a mock event
    event = Mock()
    event.name = "GW150914"
    event.repository = "/tmp/repository"
    event.meta = {"interferometers": ["H1", "L1"]}
    event.productions = []

    # Create mock productions
    production1 = Mock()
    production1.name = "production1"
    production1.rundir = "/tmp/repository/GW150914/production1"
    production1.status = "running"
    production1.meta = {}

    production2 = Mock()
    production2.name = "production2"
    production2.rundir = "/tmp/repository/GW150914/production2"
    production2.status = "finished"
    production2.meta = {}

    event.productions = [production1, production2]
    ledger.events["GW150914"] = event

    return ledger


class TestMyApplicatorHook:
    """Tests for MyApplicatorHook."""

    @pytest.fixture
    def applicator(self, mock_ledger):
        """Create an applicator hook instance."""
        return MyApplicatorHook(mock_ledger)

    def test_initialization(self, applicator, mock_ledger):
        """Test that applicator initializes correctly."""
        assert applicator.ledger == mock_ledger

    def test_run_with_valid_event(self, applicator, mock_ledger, tmp_path):
        """Test running the applicator with a valid event."""
        # Update event repository to tmp_path
        mock_ledger.events["GW150914"].repository = str(tmp_path)

        # Update production directories
        for prod in mock_ledger.events["GW150914"].productions:
            prod.rundir = str(tmp_path / "GW150914" / prod.name)

        applicator.run("GW150914")

        # Check that data directory was created
        data_dir = tmp_path / "GW150914" / "data"
        assert data_dir.exists()

        # Check that the data files were recorded under their standard names
        event = mock_ledger.events["GW150914"]
        assert event.meta["data"]["data files"] == {
            "H1": str(data_dir / "H1-strain.gwf"),
            "L1": str(data_dir / "L1-strain.gwf"),
        }
        assert set(event.meta["psds"]) == {"H1", "L1"}
        assert "data_quality" not in event.meta

    def test_run_with_invalid_event(self, applicator, caplog):
        """Test running the applicator with an invalid event."""
        applicator.run("NonExistentEvent")

        # Check that error was logged
        assert "not found in ledger" in caplog.text

    def test_record_data_files_keeps_other_data_settings(self, applicator, mock_ledger, tmp_path):
        """Recording files must not clobber existing `data:` settings."""
        event = mock_ledger.events["GW150914"]
        event.meta["data"] = {"channels": {"H1": "H1:GDS-CALIB_STRAIN"}}

        applicator._record_data_files(event, tmp_path)

        assert event.meta["data"]["channels"] == {"H1": "H1:GDS-CALIB_STRAIN"}
        assert set(event.meta["data"]["data files"]) == {"H1", "L1"}


class TestMyPostMonitorHook:
    """Tests for MyPostMonitorHook."""

    @pytest.fixture
    def monitor_hook(self, mock_ledger):
        """Create a post-monitor hook instance."""
        return MyPostMonitorHook(mock_ledger)

    def test_initialization(self, monitor_hook, mock_ledger):
        """Test that monitor hook initializes correctly."""
        assert monitor_hook.ledger == mock_ledger

    def test_run(self, monitor_hook, mock_ledger, tmp_path, monkeypatch):
        """Test running the monitor hook."""
        monkeypatch.chdir(tmp_path)
        monitor_hook.run()

        # Should complete without errors

    def test_check_completed_jobs(self, monitor_hook, mock_ledger, tmp_path):
        """Finished productions are announced once, without touching the ledger."""
        log_file = tmp_path / "completions.log"
        production = mock_ledger.events["GW150914"].productions[1]

        monitor_hook._check_completed_jobs(str(log_file))

        assert log_file.read_text() == f"GW150914,{production.name},finished\n"
        assert production.meta == {}

    def test_check_completed_jobs_already_notified(self, monitor_hook, mock_ledger, tmp_path):
        """Running twice doesn't announce the same production twice."""
        log_file = tmp_path / "completions.log"

        monitor_hook._check_completed_jobs(str(log_file))
        monitor_hook._check_completed_jobs(str(log_file))

        assert len(log_file.read_text().splitlines()) == 1

    def test_send_notification(self, monitor_hook, mock_ledger, tmp_path):
        """Test sending notifications."""
        event = mock_ledger.events["GW150914"]
        production = event.productions[1]

        # Change to tmp directory for log file
        import os

        original_dir = os.getcwd()
        os.chdir(tmp_path)

        try:
            monitor_hook._send_notification(event, production)

            # Check that log file was created
            log_file = tmp_path / "completions.log"
            assert log_file.exists()

            # Check log content
            content = log_file.read_text()
            assert "GW150914" in content
            assert production.name in content
        finally:
            os.chdir(original_dir)

    def test_cleanup_old_files(self, monitor_hook):
        """Test cleanup method."""
        # Should run without errors
        monitor_hook._cleanup_old_files()

    def test_update_tracking_system(self, monitor_hook):
        """Test tracking system update."""
        # Should run without errors
        monitor_hook._update_tracking_system()


class TestMultipleEvents:
    """Test hooks with multiple events."""

    @pytest.fixture
    def multi_event_ledger(self):
        """Create a ledger with multiple events."""
        ledger = Mock()
        ledger.events = {}

        for i, event_name in enumerate(["GW150914", "GW151226", "GW170814"]):
            event = Mock()
            event.name = event_name
            event.repository = f"/tmp/repository"
            event.meta = {}

            # Create productions with different statuses
            productions = []
            for j, status in enumerate(["ready", "running", "finished"]):
                prod = Mock()
                prod.name = f"{event_name}_prod_{j}"
                prod.rundir = f"/tmp/repository/{event_name}/prod_{j}"
                prod.status = status
                prod.meta = {}
                productions.append(prod)

            event.productions = productions
            ledger.events[event_name] = event

        return ledger

    def test_monitor_hook_with_multiple_events(self, multi_event_ledger, tmp_path, monkeypatch):
        """Test monitor hook announces every finished production."""
        monkeypatch.chdir(tmp_path)
        hook = MyPostMonitorHook(multi_event_ledger)
        hook.run()

        announced = (tmp_path / "completions.log").read_text().splitlines()
        assert len(announced) == len(multi_event_ledger.events)
        assert all(line.endswith(",finished") for line in announced)

    def test_applicator_with_multiple_events(self, multi_event_ledger, tmp_path):
        """Test applicator can process any event."""
        hook = MyApplicatorHook(multi_event_ledger)

        for event_name in multi_event_ledger.events.keys():
            event = multi_event_ledger.events[event_name]
            event.repository = str(tmp_path)

            hook.run(event_name)

            assert "data files" in event.meta["data"]
            assert "psds" in event.meta
