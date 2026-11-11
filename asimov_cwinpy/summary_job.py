"""
The job that makes the summary pages: ``python -m asimov_cwinpy.summary_job plan.json``.

It reads the plan that the ``cwinpy-summary`` pipeline wrote, checks that every file that cwinpy
is to read is there, makes the pages with cwinpy (``cwinpy.pe.summary.generate_summary_pages``),
and writes a status file saying how it went, as ``asimov_cwinpy.merge_job`` does.
"""

import json
import os
import sys
import traceback

from .merge_job import InputError, is_transient, write_status

#: The page that the others are reached from, relative to the output directory.
HOME = os.path.join("html", "home.html")


def check_inputs(paths):
    """
    Raise if a file that cwinpy is to read is not there with data in it.

    A file that is missing is transient: another analysis said it made it, so the disk it was on
    may have gone. A file that is empty is not: it was made with no data.
    """
    missing = [path for path in paths if not os.path.exists(path)]
    if missing:
        raise InputError(
            f"{len(missing)} input file(s) of the summary pages are missing, for example "
            f"{missing[0]}.",
            transient=True,
        )
    empty = [path for path in paths if os.path.getsize(path) == 0]
    if empty:
        raise InputError(
            f"{len(empty)} input file(s) of the summary pages are empty, for example {empty[0]}.",
            transient=False,
        )


def run(plan_path):
    """
    Make the pages in the plan.

    Returns
    -------
    int
        0 if the pages were made, 1 if not. The status file says which, and why.
    """
    with open(plan_path) as handle:
        plan = json.load(handle)
    status_path = plan["status"]
    home = os.path.join(plan["outpath"], HOME)
    try:
        check_inputs(plan["inputs"])
        from cwinpy.pe.summary import generate_summary_pages

        generate_summary_pages(
            config=plan["config"], outpath=plan["outpath"], url=plan["url"], **plan["options"]
        )
        if not os.path.exists(home):
            raise RuntimeError(f"cwinpy did not make {home}")
    except Exception as error:
        traceback.print_exc()  # into the error output of the job
        write_status(
            status_path,
            status="failed",
            error=f"{type(error).__name__}: {error}",
            transient=is_transient(error),
        )
        return 1
    write_status(status_path, status="succeeded", home=home)
    return 0


def main(arguments=None):
    arguments = sys.argv[1:] if arguments is None else arguments
    if len(arguments) != 1:
        raise SystemExit("usage: python -m asimov_cwinpy.summary_job plan.json")
    return run(arguments[0])


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
