"""
How much of the science data of a shard was heterodyned.

With ``strict data requirement`` false, cwinpy ignores data it cannot read and exits
successfully, and even with it true, data can be left out in other ways. A shard says what it
covered, and ``minimum coverage`` makes too little a failure.

*Covered* is the time of the heterodyned data that the output files hold: every file records
the time segments of its data (``HeterodynedDataTimeSegments``). *Science time* is the time of
the segments that the data was meant to come from, in the span of the shard: the files of
``segment lists`` if the ledger gives them, and otherwise the files that cwinpy writes into the
run directory when it makes the DAG from a segment flag.

The heterodyne itself loses a little at the start and end of each stretch of data (a filter
that has to settle, and the first and last bins), so the fraction is a little below one even
when nothing was ignored. For a job of a day it is a fraction of a percent; for a short one it
is not.
"""

import logging
import re

logger = logging.getLogger(__name__)

#: The dataset of an output file that holds the time segments of its data, ``[[start, end], ...]``.
SEGMENTS = "HeterodynedDataTimeSegments"

_NUMBERS = re.compile(r"^\s*(-?[0-9.eE+-]+)\s+(-?[0-9.eE+-]+)")


def read_segments(path):
    """
    The segments in a file of ``start end`` lines (as cwinpy writes and reads them).

    Lines that are blank, start with ``#``, or are not two numbers are ignored.

    Returns
    -------
    list of (float, float)

    Raises
    ------
    OSError
        If the file cannot be read.
    """
    found = []
    with open(path) as handle:
        for line in handle:
            match = _NUMBERS.match(line)
            if not match:
                continue
            try:
                start, end = float(match.group(1)), float(match.group(2))
            except ValueError:
                continue
            if end > start:
                found.append((start, end))
    return found


def union(segments):
    """The segments with those that overlap or touch joined, in order."""
    joined = []
    for start, end in sorted(segments):
        if joined and start <= joined[-1][1]:
            joined[-1] = (joined[-1][0], max(joined[-1][1], end))
        else:
            joined.append((start, end))
    return joined


def clip(segments, start, end):
    """The parts of the segments that are between ``start`` and ``end``."""
    return [(max(a, start), min(b, end)) for a, b in segments if b > start and a < end]


def seconds(segments):
    """The time in segments, which may overlap, counted once."""
    return sum(end - start for start, end in union(segments))


def heterodyned_seconds(paths):
    """
    The time of the data in output files of cwinpy, from the segments they record.

    Returns
    -------
    float or None
        The seconds, or None if it cannot be told: ``h5py`` is not installed, or a file
        cannot be read or does not record its segments.
    """
    try:
        import h5py
    except ImportError:
        logger.warning("h5py is not installed, so what was heterodyned cannot be measured")
        return None
    found = []
    for path in paths:
        try:
            with h5py.File(path, "r") as handle:
                found.extend((float(a), float(b)) for a, b in handle[SEGMENTS][()])
        except (OSError, KeyError, ValueError, TypeError) as error:
            logger.warning(f"The segments of {path} cannot be read: {error}")
            return None
    return seconds(found)
