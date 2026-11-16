"""
An asimov plugin for cwinpy, the known-pulsar Bayesian analysis.

Four pipelines (``cwinpy-heterodyne``, ``cwinpy-merge``, ``cwinpy-pe`` and ``cwinpy-summary``), an
analysis type (``cwinpy``) and a strategy (``cwinpy-run``) are registered with
asimov through entry points; see ``pyproject.toml`` and ``PLAN.md``.
"""

import importlib.resources

__version__ = "0.1.0"

__all__ = ["vocabulary_heterodyne", "vocabulary_pe", "vocabulary_summary"]


def vocabulary_heterodyne():
    """
    Return the ledger vocabulary of the ``cwinpy-heterodyne`` pipeline.

    Registered through the ``asimov.vocabulary`` entry point, so that
    ``asimov vocabulary check`` knows the terms under ``cwinpy-heterodyne:``.

    Returns
    -------
    importlib.abc.Traversable
        The bundled ``vocabulary_heterodyne.yaml``.
    """
    return importlib.resources.files(__name__).joinpath("vocabulary_heterodyne.yaml")


def vocabulary_pe():
    """
    Return the ledger vocabulary of the ``cwinpy-pe`` pipeline.

    Returns
    -------
    importlib.abc.Traversable
        The bundled ``vocabulary_pe.yaml``.
    """
    return importlib.resources.files(__name__).joinpath("vocabulary_pe.yaml")


def vocabulary_summary():
    """
    Return the ledger vocabulary of the ``cwinpy-summary`` pipeline.

    Returns
    -------
    importlib.abc.Traversable
        The bundled ``vocabulary_summary.yaml``.
    """
    return importlib.resources.files(__name__).joinpath("vocabulary_summary.yaml")
