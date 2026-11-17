"""
Enough of HTCondor's file transfer for the jobs that this plugin describes, without a pool.

A job that has its files transferred runs in a directory of its own, on a machine that does not
see the project: the files that it lists as inputs are copied there, the executable is copied there
and run there, and the files that it lists as outputs are copied back to where it was submitted
from. Nothing is inherited from the environment of the submitter (that is ``getenv``). This does
those things, and while the job runs it hides the paths that a node would not see, so that a job
that uses one of them fails here as it would there. It writes the events of the log that the plugin
reads.

What it does not do: matching of machines, ``requirements``, universes, a pool, or HTCondor's own
policy about files that are missing (a missing output is reported as ``held``).
"""

import contextlib
import os
import shlex
import shutil
import signal
import subprocess
import tempfile
import time
from types import SimpleNamespace


def names(text):
    """The files of a comma separated list, as HTCondor reads it."""
    return [name.strip() for name in (text or "").split(",") if name.strip()]


def variables(text):
    """The variables of HTCondor's ``environment``, in its own syntax (``"A=1 B='two words'"``)."""
    found = {}
    for pair in shlex.split((text or "").strip().strip('"')):
        name, _, value = pair.partition("=")
        found[name] = value
    return found


@contextlib.contextmanager
def hidden(*paths):
    """Make paths not exist for a while, as the project does not on an execute node."""
    moved = []
    try:
        for path in paths:
            target = f"{path}.not-on-the-node"
            os.rename(path, target)
            moved.append((path, target))
        yield
    finally:
        for path, target in reversed(moved):
            os.rename(target, path)


def event(number, text, cluster=1):
    return f"{number:03d} ({cluster:03d}.000.000) 01/01 00:00:00 {text}\n...\n"


def _send_inputs(description, scratch):
    """Copy the input files, as HTCondor does, flat, into the directory of the job."""
    for path in names(description.get("transfer_input_files")):
        target = os.path.join(scratch, os.path.basename(path.rstrip(os.sep)))
        if os.path.isdir(path):
            shutil.copytree(path, target)
        else:
            shutil.copy2(path, target)


def _environment(description, scratch):
    """What the job is given: nothing of the submitter's, unless it asked for it."""
    environment = {
        "PATH": "/usr/bin:/bin",
        "TMPDIR": scratch,
        "_CONDOR_SCRATCH_DIR": scratch,
        **variables(description.get("environment")),
    }
    if description.get("getenv") in ("true", "True", True):
        environment = {**os.environ, **environment}
    return environment


def _bring_back(description, scratch):
    """Copy the output files back to where the job was submitted from; say which are missing."""
    initial = description.get("initialdir") or os.getcwd()
    missing = []
    for name in names(description.get("transfer_output_files")):
        source = os.path.join(scratch, name)
        destination = os.path.join(initial, name)
        if not os.path.exists(source):
            missing.append(name)
        elif os.path.isdir(source):
            shutil.rmtree(destination, ignore_errors=True)
            shutil.copytree(source, destination)
        else:
            shutil.copy2(source, destination)
    return missing


def _log(job, text):
    if job.log:
        with open(job.log, "a") as handle:
            handle.write(text)


def run_job(job, hide=(), terminate_after=None, cluster=1):
    """
    Run a job as it would be on a node that has its files transferred.

    Parameters
    ----------
    job : asimov.scheduler.JobDescription
        As the plugin gave it to the scheduler.
    hide : sequence of str
        The paths that the node does not see: they do not exist while the job runs.
    terminate_after : float, optional
        Send the job SIGTERM after this many seconds, as when it is evicted.
    cluster : int
        The number in the events that are written to the log.

    Returns
    -------
    types.SimpleNamespace
        ``returncode``, ``scratch`` (the names in the directory of the job when it started),
        ``held`` (the outputs that it listed and did not make), ``stdout`` and ``stderr``.
    """
    description = job.to_htcondor()
    scratch = tempfile.mkdtemp(prefix="condor-scratch-")
    _send_inputs(description, scratch)
    executable = os.path.join(scratch, os.path.basename(job.executable))
    shutil.copy2(job.executable, executable)  # transfer_executable is on unless it is told not
    started = sorted(os.listdir(scratch))
    _log(job, event(0, "Job submitted from host: <127.0.0.1>", cluster))

    stdout, stderr = (os.path.join(scratch, "_stdout"), os.path.join(scratch, "_stderr"))
    with hidden(*hide), open(stdout, "w") as out, open(stderr, "w") as err:
        process = subprocess.Popen(
            [executable, *shlex.split(description.get("arguments", ""))],
            cwd=scratch,
            env=_environment(description, scratch),
            stdout=out,
            stderr=err,
        )
        if terminate_after is not None:
            time.sleep(terminate_after)
            process.send_signal(signal.SIGTERM)
        returncode = process.wait(timeout=600)

    held = _bring_back(description, scratch)
    for source, destination in ((stdout, job.output), (stderr, job.error)):
        if destination:
            shutil.copy2(source, destination)
    ended = f"Job terminated.\n\t(1) Normal termination (return value {returncode})"
    _log(job, event(5, ended, cluster))
    result = SimpleNamespace(
        returncode=returncode,
        scratch=started,
        held=held,
        stdout=open(stdout).read(),
        stderr=open(stderr).read(),
    )
    shutil.rmtree(scratch, ignore_errors=True)
    return result
