"""
Asimov plugin template.

This package provides a template for creating asimov plugins with support for:
- Custom pipelines
- Applicator hooks
- Post-monitor hooks
"""

__version__ = "0.1.0"

import importlib.resources

from .pipeline import MyPlugin

__all__ = ["MyPlugin", "vocabulary"]


def vocabulary():
    """
    Return this plugin's ledger vocabulary.

    Registered through the ``asimov.vocabulary`` entry point, so that
    ``asimov vocabulary check --pipeline myplugin`` knows the terms under
    ``myplugin:``.

    Returns
    -------
    importlib.abc.Traversable
        The bundled ``vocabulary.yaml``.
    """
    return importlib.resources.files(__name__).joinpath("vocabulary.yaml")
