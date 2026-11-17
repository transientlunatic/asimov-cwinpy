"""
What the jobs of the cwinpy pipelines say to HTCondor, other than what each one runs.

The cluster that the jobs run on is not the same everywhere, and these are the ways that the ledger
says how it differs, all optional:

``scheduler: environment``
    The software environment that the analysis runs in: a directory with ``bin/`` in it, like
    ``[pipelines] environment`` in the configuration of the project, which it replaces for this
    analysis. On a cluster whose execute nodes do not see the project (nor the home directory
    that the environment of a user is in) this is one that they do, such as an IGWN environment
    on CVMFS.
``scheduler: environment variables``
    Variables that are set in the job. Nothing is inherited from the environment of asimov (that
    is ``getenv``, which some pools do not allow), so what the job needs is said here.
``htcondor: universe``
    ``local``, to run a job on the machine that asimov runs on, where the project is.
``htcondor: requirements``
    A ClassAd expression that the machine has to satisfy.
``htcondor: getenv``
    true, for a pool that allows it, to give the job the environment of asimov.
"""

import os

from asimov import config

#: What ``htcondor:`` and ``scheduler:`` may say here, for the vocabulary of the ledger.
HTCONDOR_TERMS = {
    "universe": {
        "type": "string",
        "description": (
            "The HTCondor universe of the jobs that asimov describes itself (merge, summary, "
            "parameter estimation). `local` runs a job on the machine that asimov runs on, "
            "where the project is, so that it needs no file transfer, and is for the light ones. "
            "The jobs of the DAG that cwinpy makes are vanilla."
        ),
    },
    "requirements": {
        "type": "string",
        "description": (
            "A ClassAd expression that the machine that runs a job has to satisfy, such as "
            "`(HAS_CVMFS_software_igwn_org =?= True)`."
        ),
    },
    "getenv": {
        "type": "boolean",
        "description": (
            "Give the jobs the environment of asimov (HTCondor's getenv). Off unless set, because "
            "some pools do not allow it: use `scheduler: environment variables` to say what a job "
            "needs."
        ),
    },
}


def software_environment(production):
    """
    The directory of the software environment that an analysis runs in.

    Returns
    -------
    str or None
        ``scheduler: environment`` of the analysis, else ``[pipelines] environment`` of the
        project, else None.
    """
    asked = (production.meta.get("scheduler") or {}).get("environment")
    if asked:
        return str(asked)
    try:
        return config.get("pipelines", "environment")
    except Exception:
        return None


def executable_in(production, name):
    """The path of ``name`` in the ``bin`` of the software environment, or None if there is none."""
    environment = software_environment(production)
    return os.path.join(environment, "bin", name) if environment else None


def format_environment(variables):
    """
    The value of HTCondor's ``environment`` for a mapping of variables.

    HTCondor's own syntax: the pairs separated by spaces, inside double quotes, a value with a
    space or a quote in it inside single quotes, and quotes doubled inside.
    """
    pairs = []
    for name, value in variables.items():
        text = str(value).replace("'", "''").replace('"', '""')
        if any(character in text for character in " \t'") or not text:
            text = f"'{text}'"
        pairs.append(f"{name}={text}")
    return '"' + " ".join(pairs) + '"'


def htcondor_options(production):
    """
    The lines of a submit description that ``htcondor:`` and ``scheduler:`` ask for.

    Returns
    -------
    dict
        ``universe``, ``requirements``, ``getenv`` and ``environment``, for those that are set.
    """
    htcondor = production.meta.get("htcondor") or {}
    scheduler = production.meta.get("scheduler") or {}
    options = {}
    if htcondor.get("universe"):
        options["universe"] = str(htcondor["universe"])
    if htcondor.get("requirements"):
        options["requirements"] = str(htcondor["requirements"])
    if htcondor.get("getenv"):
        options["getenv"] = "true"
    if scheduler.get("environment variables"):
        options["environment"] = format_environment(scheduler["environment variables"])
    return options


def runs_locally(production):
    """Whether the job runs on the machine that asimov runs on (``htcondor: universe: local``)."""
    return (production.meta.get("htcondor") or {}).get("universe") == "local"
