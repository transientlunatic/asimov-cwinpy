"""
Where the ephemerides of a run come from: files, directories of them, and the ATNF catalogue.

A pulsar of the ``cwinpy-run`` strategy has an ephemeris, a Tempo(2)-style ``.par`` file, which the
heterodyne and the parameter estimation both read as a file. So ephemerides that are not files yet
are made into files, in a directory of the project (``ephemerides``, unless the blueprint says), and
the rest of the plugin only ever sees paths:

* a file, as it is;
* a directory, which is every ``.par`` file in it and under it;
* a pulsar's name with ``source: atnf``, which is fetched once from the ATNF pulsar catalogue
  with ``psrqpy`` and written there as ``<name>.par``, with ``<name>.atnf.json`` that says what the
  catalogue and the client were. A file that is already there is used and not fetched again, so a
  run can be applied again without the network, and a timing solution of your own put there is
  not replaced.

The ephemerides of the catalogue are those of its entries, which say where a pulsar is, and are not
timing solutions that follow the phase of a pulsar through a run. They are for trying the plugin.
"""

import datetime
import json
import os
import re
from pathlib import Path

from asimov.strategies import StrategyError

#: Where the ephemerides that are fetched go, in the project, unless the blueprint says.
DIRECTORY = "ephemerides"

#: The line of a ``.par`` file that names the pulsar, in the order that cwinpy prefers.
_NAME = re.compile(r"^\s*(PSRJ|PSR|NAME)\s+(\S+)", re.IGNORECASE)


def pulsar_name(path):
    """
    The name of the pulsar in a ``.par`` file: its ``PSRJ``, else ``PSR``, else ``NAME``.

    Raises
    ------
    StrategyError
        If the file cannot be read or names no pulsar.
    """
    try:
        with open(path) as handle:
            found = {}
            for line in handle:
                match = _NAME.match(line)
                if match:
                    found.setdefault(match.group(1).upper(), match.group(2))
    except OSError as error:
        raise StrategyError(f"The ephemeris {path} cannot be read: {error}.") from error
    for key in ("PSRJ", "PSR", "NAME"):
        if key in found:
            return found[key]
    raise StrategyError(f"The ephemeris {path} does not name its pulsar (PSRJ).")


def par_files(directory):
    """
    The ``.par`` files in a directory and under it, in the order of their paths.

    Raises
    ------
    StrategyError
        If there are none.
    """
    found = sorted(str(path) for path in Path(directory).rglob("*.par") if path.is_file())
    if not found:
        raise StrategyError(f"There is no .par file in the directory {directory}.")
    return found


def fetch_atnf(name):
    """
    The ephemeris of a pulsar from the ATNF pulsar catalogue.

    Returns
    -------
    tuple of (str, str, str)
        The text of the ephemeris, the version of the catalogue, and the version of ``psrqpy``.

    Raises
    ------
    StrategyError
        If ``psrqpy`` is not installed, the catalogue cannot be reached, or it has no such pulsar.
    """
    try:
        import psrqpy
    except ImportError as error:
        raise StrategyError(
            "psrqpy, which gets ephemerides from the ATNF catalogue and which cwinpy needs, is not "
            "installed (pip install psrqpy)."
        ) from error
    try:
        query = psrqpy.QueryATNF(psrs=[name])
        text = query.get_ephemeris(name)
        version = str(query.get_version)
    except Exception as error:
        raise StrategyError(
            f"The ephemeris of {name} could not be got from the ATNF catalogue: {error}"
        ) from error
    if not text:
        raise StrategyError(f"The ATNF catalogue has no pulsar {name}.")
    return text, version, str(psrqpy.__version__)


def atnf_path(name, directory):
    """Where the ephemeris of a pulsar from the catalogue is (or will be), as an absolute path."""
    return os.path.join(os.path.abspath(directory), f"{name}.par")


def ensure_atnf(name, directory, logger=None):
    """
    The path of the ephemeris of a pulsar from the catalogue, fetching it if it is not there.

    A file that is there already is used as it is.

    Returns
    -------
    str
        The path.
    """
    path = atnf_path(name, directory)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path
    text, version, client = fetch_atnf(name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary = f"{path}.tmp"
    with open(temporary, "w") as handle:
        handle.write(text if text.endswith("\n") else text + "\n")
    os.replace(temporary, path)
    with open(os.path.join(os.path.dirname(path), f"{name}.atnf.json"), "w") as handle:
        json.dump(
            {
                "pulsar": name,
                "source": "ATNF pulsar catalogue",
                "catalogue version": version,
                "psrqpy": client,
                "fetched": datetime.datetime.now(datetime.timezone.utc).isoformat(
                    timespec="seconds"
                ),
            },
            handle,
            indent=1,
        )
    if logger is not None:
        logger.info(f"Fetched the ephemeris of {name} from the ATNF catalogue {version}: {path}")
    return path
