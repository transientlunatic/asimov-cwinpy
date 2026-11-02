"""
Example hook implementations for asimov.

This module demonstrates how to create applicator hooks and post-monitor hooks
that integrate with asimov's workflow.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class MyApplicatorHook:
    """
    Example applicator hook for asimov.

    Applicator hooks are used to provide data to asimov projects.
    They are invoked with: asimov apply -p my-applicator -e <event_name>

    Use cases:
    - Fetch data from external sources (e.g., GWOSC, LIGO data grid)
    - Generate configuration files from templates
    - Set up initial analysis parameters
    - Copy or download required files

    Attributes
    ----------
    ledger : asimov.ledger.Ledger
        The asimov ledger object
    """

    def __init__(self, ledger):
        """
        Initialize the applicator hook.

        Parameters
        ----------
        ledger : asimov.ledger.Ledger
            The asimov ledger object that manages all events and productions
        """
        self.ledger = ledger
        self.logger = logger
        self.logger.info("Initialized MyApplicatorHook")

    def run(self, event):
        """
        Run the applicator hook for a specific event.

        This method is called when: asimov apply -p my-applicator -e <event_name>

        Parameters
        ----------
        event : str
            The name of the event to apply the hook to

        Examples
        --------
        The hook might:
        1. Fetch strain data for the event
        2. Generate pipeline configuration files
        3. Set up directory structure
        4. Download reference files (PSDs, calibration files, etc.)
        """
        self.logger.info(f"Running applicator hook for event: {event}")

        # Get the event object from the ledger
        try:
            event_obj = self.ledger.events[event]
        except KeyError:
            self.logger.error(f"Event '{event}' not found in ledger")
            return

        # Example: Set up data directories
        event_dir = Path(event_obj.repository) / event
        data_dir = event_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Created data directory: {data_dir}")

        # Example: Fetch data from an external source
        # self._fetch_strain_data(event_obj, data_dir)

        # Example: Generate configuration files
        # self._generate_config_files(event_obj, event_dir)

        # Example: record the fetched files under their *standard* ledger
        # names, so every pipeline finds them. Strain files go in
        # `data: data files` and PSDs in `psds`, both keyed by detector.
        # Never invent keys (`data_files`, `psd_files`, `data_quality`...):
        # `asimov vocabulary show "data.data files"` lists the standard ones.
        self._record_data_files(event_obj, data_dir)

        self.logger.info(f"Applicator hook completed for {event}")

    def _record_data_files(self, event, data_dir):
        """
        Record fetched strain and PSD files on the event.

        Analyses inherit these from the event, and a pipeline reads the
        PSDs back through ``production.psds`` (which also resolves PSDs
        supplied by an upstream analysis through ``needs:``).

        Parameters
        ----------
        event : asimov.event.Event
            The event whose data were fetched.
        data_dir : Path
            The directory containing the data files.
        """
        ifos = event.meta.get("interferometers", [])
        event.meta.setdefault("data", {})["data files"] = {
            ifo: str(data_dir / f"{ifo}-strain.gwf") for ifo in ifos
        }
        event.meta["psds"] = {ifo: str(data_dir / f"{ifo}-psd.dat") for ifo in ifos}

    def _fetch_strain_data(self, event, output_dir):
        """
        Example method to fetch strain data.

        Parameters
        ----------
        event : asimov.event.Event
            The event object
        output_dir : Path
            Directory to save the data
        """
        # Example implementation:
        # 1. Use gwpy or gwosc to fetch data
        # 2. Save to HDF5 or other format
        # 3. Update event metadata with file locations
        pass

    def _generate_config_files(self, event, output_dir):
        """
        Example method to generate configuration files.

        Parameters
        ----------
        event : asimov.event.Event
            The event object
        output_dir : Path
            Directory to save configuration files
        """
        # Example implementation:
        # 1. Read template configuration
        # 2. Fill in event-specific parameters
        # 3. Write configuration file
        pass


class MyPostMonitorHook:
    """
    Example post-monitor hook for asimov.

    Post-monitor hooks are called after each monitoring cycle completes.
    They are configured in the ledger YAML under: hooks/postmonitor

    Use cases:
    - Send notifications about job status changes
    - Update external databases or tracking systems
    - Generate progress reports
    - Trigger downstream workflows
    - Clean up completed jobs
    - Archive results

    Attributes
    ----------
    ledger : asimov.ledger.Ledger
        The asimov ledger object
    """

    def __init__(self, ledger):
        """
        Initialize the post-monitor hook.

        Parameters
        ----------
        ledger : asimov.ledger.Ledger
            The asimov ledger object that manages all events and productions
        """
        self.ledger = ledger
        self.logger = logger
        self.logger.info("Initialized MyPostMonitorHook")

    def run(self):
        """
        Run the post-monitor hook.

        This method is called after each monitoring cycle.
        It has access to the full ledger with all events and productions.

        Examples
        --------
        The hook might:
        1. Check for newly completed jobs and send notifications
        2. Update a status dashboard
        3. Archive completed results
        4. Clean up old files
        5. Trigger next steps in a workflow
        """
        self.logger.info("Running post-monitor hook")

        # Example: Count jobs in different states
        job_counts = {
            "running": 0,
            "completed": 0,
            "failed": 0,
            "ready": 0,
        }

        # Iterate through all events and productions
        for event in self.ledger.events.values():
            for production in event.productions:
                status = production.status
                if status in job_counts:
                    job_counts[status] += 1

        self.logger.info(f"Job status summary: {job_counts}")

        # Example: Send notifications for completed jobs
        self._check_completed_jobs()

        # Example: Clean up old files
        # self._cleanup_old_files()

        # Example: Update external tracking system
        # self._update_tracking_system()

        self.logger.info("Post-monitor hook completed")

    def _check_completed_jobs(self, log_file="completions.log"):
        """
        Send one notification per newly finished production.

        Which productions have already been announced is read back from the
        completion log this hook writes, not from a flag in the ledger: the
        ledger holds analysis inputs and asimov-managed state, not a hook's
        bookkeeping.

        Parameters
        ----------
        log_file : str, optional
            The completion log written by ``_send_notification``.
        """
        try:
            with open(log_file) as handle:
                notified = {tuple(line.split(",")[:2]) for line in handle if line.strip()}
        except FileNotFoundError:
            notified = set()
        for event in self.ledger.events.values():
            for production in event.productions:
                if production.status != "finished":
                    continue
                if (event.name, production.name) not in notified:
                    self._send_notification(event, production, log_file)

    def _send_notification(self, event, production, log_file="completions.log"):
        """
        Send a notification about a completed job.

        Parameters
        ----------
        event : asimov.event.Event
            The event object
        production : asimov.event.Production
            The completed production
        log_file : str, optional
            The completion log to append to.
        """
        self.logger.info(f"Production completed: {event.name}/{production.name}")

        # Example implementations:
        # 1. Send email notification
        # 2. Post to Slack/Mattermost
        # 3. Update a web dashboard
        # 4. Write to a log file or database

        # Example: Write to a completion log
        with open(log_file, "a") as f:
            f.write(f"{event.name},{production.name},{production.status}\n")

    def _cleanup_old_files(self):
        """
        Example method to clean up old or unnecessary files.
        """
        # Example implementation:
        # 1. Identify old completed jobs
        # 2. Archive or delete intermediate files
        # 3. Keep only essential results
        pass

    def _update_tracking_system(self):
        """
        Example method to update an external tracking system.
        """
        # Example implementation:
        # 1. Connect to external database or API
        # 2. Update job status information
        # 3. Upload summary statistics
        pass


class MyCustomHook:
    """
    Example of a custom hook with specific functionality.

    You can create multiple hook classes in this file for different purposes.
    Each one should be registered in pyproject.toml under the appropriate
    entry point group.
    """

    def __init__(self, ledger):
        """
        Initialize the custom hook.

        Parameters
        ----------
        ledger : asimov.ledger.Ledger
            The asimov ledger object
        """
        self.ledger = ledger
        self.logger = logger

    def run(self, *args, **kwargs):
        """
        Run the custom hook.

        Implement your custom logic here.
        """
        self.logger.info("Running custom hook")
        # Your custom implementation
