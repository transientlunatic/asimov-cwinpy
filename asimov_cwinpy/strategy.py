"""
The ``cwinpy-run`` strategy.

One blueprint describes an observing run, and the strategy expands it into everything that
analysing it takes:

* a *run subject*, which holds the heterodyne of the whole run: its shards
  (``cwinpy-heterodyne``, one per stretch of the data) and the merge of them
  (``cwinpy-merge``);
* a *subject for each pulsar*, with a parameter estimation (``cwinpy-pe``) which needs the merge.

Every name comes from the blueprint, so applying it again changes nothing that exists.

.. code-block:: yaml

    kind: analysis
    name: O4a
    strategy:
      type: cwinpy-run
      interferometers: [H1, L1]
      data start: 1368975618
      data end: 1380000000
      shards: {count: 60}
      pulsars:
        - /path/to/J0534+2200.par
        - /path/to/J0537-6910.par
      heterodyne:
        data: {channels: ..., frame types: ..., segments: ..., datafind url: ...}
        retry: {attempts: 10, delay: 3600}

Pulsars that are added later are given a ``batch``, a name for the set that they are
heterodyned together in. A batch has shards and a merge of its own, so the data of the
pulsars that are already done is not made again.
"""

import math
import re
from copy import deepcopy

from asimov.strategies import Strategy, StrategyError

#: The length of the data that one job of a shard reads, if the blueprint does not say
#: (cwinpy's default).
JOBLENGTH = 86400

#: The batch of the pulsars the blueprint lists as plain file names.
MAIN = "main"

OPTIONS = {
    "type",
    "run",
    "interferometers",
    "frequency factors",
    "data start",
    "data end",
    "joblength",
    "shards",
    "pulsars",
    "heterodyne",
    "merge",
    "pe",
}
HETERODYNE_OPTIONS = {"data", "settings", "scheduler", "retry"}
MERGE_OPTIONS = {"scheduler", "retry"}
PE_OPTIONS = {"settings", "sampler", "priors", "scheduler", "retry"}

#: The line of a ``.par`` file that names the pulsar, in the order that cwinpy prefers.
_NAME = re.compile(r"^\s*(PSRJ|PSR|NAME)\s+(\S+)", re.IGNORECASE)
_SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.+-]*$")


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


def _positive_int(value, what):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise StrategyError(f"'{what}' must be a whole number of at least 1, not {value!r}.")
    return value


def _mapping(spec, key, allowed):
    block = spec.get(key) or {}
    if not isinstance(block, dict):
        raise StrategyError(f"'{key}' must be a mapping.")
    unknown = set(block) - allowed
    if unknown:
        raise StrategyError(
            f"Unknown option(s) in '{key}': {', '.join(sorted(map(str, unknown)))}. "
            f"It can have: {', '.join(sorted(allowed))}."
        )
    return block


def shard_ranges(start, end, joblength, shards):
    """
    Split the data of the run into the stretches that the shards read.

    A shard is a whole number of jobs, so that no job is cut short but the last.

    Parameters
    ----------
    start, end : int
        The GPS span of the run.
    joblength : int
        The length of data that one job reads.
    shards : dict
        ``{count: n}``, for about ``n`` shards, or ``{length: s}``, for shards of at least ``s``
        seconds. (Fewer shards than ``count`` if there are fewer jobs than that.)

    Returns
    -------
    list of (int, int)
        The start and end of each shard.
    """
    jobs = math.ceil((end - start) / joblength)
    if "count" in shards:
        per_shard = math.ceil(jobs / shards["count"])
    else:
        per_shard = max(1, math.ceil(shards["length"] / joblength))
    length = per_shard * joblength
    return [(first, min(first + length, end)) for first in range(start, end, length)]


class RunStrategy(Strategy):
    """Expand an observing run into shards, a merge, and one analysis per pulsar."""

    name = "cwinpy-run"

    # What is asked for

    def validate(self, spec):
        unknown = set(spec) - OPTIONS
        if unknown:
            raise StrategyError(
                f"Unknown option(s) for cwinpy-run: {', '.join(sorted(map(str, unknown)))}."
            )
        for key in ("interferometers", "data start", "data end", "shards", "pulsars"):
            if not spec.get(key):
                raise StrategyError(f"cwinpy-run needs '{key}'.")
        self._validate_detectors(spec)
        self._validate_span(spec)
        self._validate_factors(spec)
        _mapping(spec, "heterodyne", HETERODYNE_OPTIONS)
        _mapping(spec, "merge", MERGE_OPTIONS)
        _mapping(spec, "pe", PE_OPTIONS)
        self.pulsars(spec)

    @staticmethod
    def _validate_detectors(spec):
        detectors = spec["interferometers"]
        if (
            not isinstance(detectors, list)
            or not all(isinstance(d, str) for d in detectors)
            or len(set(detectors)) != len(detectors)
        ):
            raise StrategyError("'interferometers' must be a list of different detectors.")

    @staticmethod
    def _validate_span(spec):
        for key in ("data start", "data end"):
            if isinstance(spec[key], bool) or not isinstance(spec[key], int):
                raise StrategyError(f"'{key}' must be a GPS time in whole seconds.")
        if spec["data end"] <= spec["data start"]:
            raise StrategyError("'data end' must be after 'data start'.")
        if "joblength" in spec:
            _positive_int(spec["joblength"], "joblength")
        shards = spec["shards"]
        if not isinstance(shards, dict) or set(shards) not in ({"count"}, {"length"}):
            raise StrategyError("'shards' must be {count: n} or {length: seconds}, one of them.")
        _positive_int(next(iter(shards.values())), f"shards: {next(iter(shards))}")

    @staticmethod
    def _validate_factors(spec):
        factors = spec.get("frequency factors")
        if factors is not None and (
            not isinstance(factors, list)
            or not factors
            or not all(isinstance(f, (int, float)) and not isinstance(f, bool) for f in factors)
        ):
            raise StrategyError("'frequency factors' must be a list of numbers.")

    def pulsars(self, spec):
        """
        The pulsars of the run.

        Returns
        -------
        dict
            ``{batch: [(name, ephemeris), ...]}``, with the pulsars of a batch in the order
            that they were listed.

        Raises
        ------
        StrategyError
            If an entry is not understood, a name is used twice, or a file cannot be read.
        """
        entries = spec["pulsars"]
        if not isinstance(entries, list):
            raise StrategyError("'pulsars' must be a list.")
        batches, seen = {}, {}
        for entry in entries:
            if isinstance(entry, str):
                path, batch, name = entry, MAIN, None
            elif isinstance(entry, dict) and set(entry) <= {"ephemeris", "batch", "name"}:
                if not isinstance(entry.get("ephemeris"), str):
                    raise StrategyError(f"A pulsar needs its 'ephemeris': {entry!r}.")
                path, batch, name = entry["ephemeris"], entry.get("batch", MAIN), entry.get("name")
            else:
                raise StrategyError(
                    f"A pulsar is a file name, or a mapping of ephemeris, batch and name, "
                    f"not {entry!r}."
                )
            name = name or pulsar_name(path)
            for label, text in (("pulsar", name), ("batch", batch)):
                if not isinstance(text, str) or not _SAFE.match(text):
                    raise StrategyError(
                        f"The {label} name {text!r} is not usable: it has to start with a letter "
                        "or digit and have only letters, digits, and _ . + - after."
                    )
            if name in seen:
                where = "twice" if seen[name] == batch else f"in batches {seen[name]} and {batch}"
                raise StrategyError(f"The pulsar {name} is listed {where}.")
            seen[name] = batch
            batches.setdefault(batch, []).append((name, path))
        return batches

    # What it makes

    @staticmethod
    def _tail(block, *keys):
        """The settings in a block, copied, for the keys of it that are set."""
        return {key: deepcopy(block[key]) for key in keys if block.get(key) is not None}

    def expand(self, blueprint, context):
        spec = blueprint["strategy"]
        run = spec.get("run") or f"{blueprint['name']}-heterodyne"
        detectors = list(spec["interferometers"])
        factors = list(spec.get("frequency factors") or [1, 2])
        joblength = spec.get("joblength", JOBLENGTH)
        heterodyne = _mapping(spec, "heterodyne", HETERODYNE_OPTIONS)
        merge = _mapping(spec, "merge", MERGE_OPTIONS)
        pe = _mapping(spec, "pe", PE_OPTIONS)
        batches = self.pulsars(spec)
        ranges = shard_ranges(spec["data start"], spec["data end"], joblength, spec["shards"])

        documents = [
            {
                "kind": "event",
                "name": run,
                "interferometers": detectors,
                "frequency factors": factors,
            }
        ]
        merges = {}
        for batch, members in sorted(batches.items(), key=lambda item: (item[0] != MAIN, item[0])):
            prefix = "" if batch == MAIN else f"{batch}-"
            names = []
            for number, (first, last) in enumerate(ranges, start=1):
                shard = {
                    "kind": "analysis",
                    "name": f"{prefix}shard-{number:04d}",
                    "pipeline": "cwinpy-heterodyne",
                    "event": run,
                    **self._tail(heterodyne, "data", "scheduler", "retry"),
                    "cwinpy-heterodyne": {
                        **deepcopy(heterodyne.get("settings") or {}),
                        "data start": first,
                        "data end": last,
                        "joblength": joblength,
                        "ephemerides": [path for _, path in members],
                    },
                }
                names.append(shard["name"])
                documents.append(shard)
            merges[batch] = f"merge-{batch}" if batch != MAIN else "merge"
            documents.append(
                {
                    "kind": "analysis",
                    "name": merges[batch],
                    "pipeline": "cwinpy-merge",
                    "event": run,
                    "needs": names,
                    **self._tail(merge, "scheduler", "retry"),
                }
            )

        for batch, members in sorted(batches.items(), key=lambda item: (item[0] != MAIN, item[0])):
            for name, path in members:
                subject = {
                    "kind": "event",
                    "name": name,
                    "ephemeris": path,
                    "interferometers": detectors,
                    "frequency factors": factors,
                }
                if pe.get("priors"):
                    subject["priors"] = deepcopy(pe["priors"])
                documents.append(subject)
                documents.append(
                    {
                        "kind": "analysis",
                        "name": "pe",
                        "pipeline": "cwinpy-pe",
                        "event": name,
                        "needs": [f"{run}/{merges[batch]}"],
                        **self._tail(pe, "sampler", "scheduler", "retry"),
                        **({"cwinpy-pe": deepcopy(pe["settings"])} if pe.get("settings") else {}),
                    }
                )
        return documents
