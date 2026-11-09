"""
The job that merges heterodyned files: ``python -m asimov_cwinpy.merge_job plan.json``.

It reads the plan that the ``cwinpy-merge`` pipeline wrote, and for each pulsar, detector
and frequency factor merges the files of the shards into one with cwinpy, then writes a
status file saying how it went. All the merges are done by one process, because starting
cwinpy takes longer than a merge does.

``heterodyne_merge`` of cwinpy drops any file it is given that does not exist, so it
would merge what is left and say nothing. Every input is checked here first: a file that
is missing or empty is an error, and no merge is made from the rest.

The status file is how the pipeline knows what happened, whichever scheduler ran the job;
a job that dies without writing one has left no evidence, and is not taken to have failed.
"""

import errno
import json
import os
import sys
import traceback

#: The errors of reading or writing a file that come from the file system being unwell,
#: not from the file: a disk or a mount which comes back.
TRANSIENT_ERRNO = {
    errno.EIO,
    errno.ESTALE,
    errno.ETIMEDOUT,
    errno.ECONNRESET,
    errno.EHOSTDOWN,
    errno.ENETDOWN,
    errno.ENOTCONN,
}


class InputError(Exception):
    """An input file of a merge is missing or empty."""

    def __init__(self, message, transient):
        super().__init__(message)
        self.transient = transient


def is_transient(error):
    """Whether an error is plausibly one that trying again will not repeat."""
    if isinstance(error, InputError):
        return error.transient
    return isinstance(error, OSError) and error.errno in TRANSIENT_ERRNO


def write_status(path, **status):
    """Write the status file, all at once, so that it is never seen half written."""
    temporary = f"{path}.tmp"
    with open(temporary, "w") as handle:
        json.dump(status, handle, indent=1)
    os.replace(temporary, path)


def check_inputs(item):
    """
    Raise if an input of a merge is not there with data in it.

    A file that is missing is transient: the shard said it made it, so the disk it was on
    may have gone. A file that is empty is not: it was made with no data.
    """
    missing = [path for path in item["inputs"] if not os.path.exists(path)]
    if missing:
        raise InputError(
            f"{len(missing)} input file(s) of the merge for {item['pulsar']} {item['detector']} "
            f"{item['factor']}f are missing, for example {missing[0]}.",
            transient=True,
        )
    empty = [path for path in item["inputs"] if os.path.getsize(path) == 0]
    if empty:
        raise InputError(
            f"{len(empty)} input file(s) of the merge for {item['pulsar']} {item['detector']} "
            f"{item['factor']}f are empty, for example {empty[0]}.",
            transient=False,
        )


def run(plan_path):
    """
    Make every merge in the plan.

    Returns
    -------
    int
        0 if every merge was made, 1 if not. The status file says which, and why.
    """
    with open(plan_path) as handle:
        plan = json.load(handle)
    status_path = plan["status"]
    try:
        for item in plan["items"]:
            check_inputs(item)
        from cwinpy.heterodyne import heterodyne_merge

        for item in plan["items"]:
            os.makedirs(os.path.dirname(item["output"]), exist_ok=True)
            heterodyne_merge(heterodynedfiles=item["inputs"], output=item["output"], overwrite=True)
    except Exception as error:
        traceback.print_exc()  # into the error output of the job
        write_status(
            status_path,
            status="failed",
            error=f"{type(error).__name__}: {error}",
            transient=is_transient(error),
        )
        return 1
    write_status(status_path, status="succeeded", merged=len(plan["items"]))
    return 0


def main(arguments=None):
    arguments = sys.argv[1:] if arguments is None else arguments
    if len(arguments) != 1:
        raise SystemExit("usage: python -m asimov_cwinpy.merge_job plan.json")
    return run(arguments[0])


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
