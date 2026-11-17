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
import os
import re
from copy import deepcopy

from asimov.strategies import Strategy, StrategyError

from .ephemerides import DIRECTORY, atnf_path, ensure_atnf, par_files, pulsar_name

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
    "ephemeris directory",
    "shared filesystem",
    "split detectors",
    "heterodyne",
    "merge",
    "pe",
    "summary",
}
HETERODYNE_OPTIONS = {"data", "settings", "scheduler", "htcondor", "retry"}
MERGE_OPTIONS = {"scheduler", "htcondor", "retry"}
PE_OPTIONS = {"settings", "sampler", "priors", "scheduler", "htcondor", "retry"}
SUMMARY_OPTIONS = {"settings", "scheduler", "htcondor", "retry"}

_SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.+-]*$")

#: What a mapping that is a pulsar may have.
ENTRY_KEYS = {"ephemeris", "batch", "name", "source"}


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
        self.summary(spec)
        directory = spec.get("ephemeris directory", DIRECTORY)
        if not isinstance(directory, str) or not directory:
            raise StrategyError("'ephemeris directory' must be a path.")
        for key in ("shared filesystem", "split detectors"):
            if not isinstance(spec.get(key, key == "shared filesystem"), bool):
                raise StrategyError(f"'{key}' must be true or false.")
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

    @staticmethod
    def summary(spec):
        """
        What is asked of the summary pages.

        Returns
        -------
        dict or None
            The ``summary`` block (empty if it is just ``true``), or None if there are to be no
            summary pages.

        Raises
        ------
        StrategyError
            If it is not true, false or a mapping, or has an option that it does not.
        """
        asked = spec.get("summary")
        if asked is None or asked is False:
            return None
        if asked is True:
            return {}
        return _mapping(spec, "summary", SUMMARY_OPTIONS)

    def pulsars(self, spec, fetch=False, logger=None):
        """
        The pulsars of the run.

        Parameters
        ----------
        spec : dict
            The ``strategy`` block.
        fetch : bool
            Fetch the ephemerides that are from the ATNF catalogue and are not in the project
            yet. Otherwise only where they will be is worked out, so that the options can be
            checked without the network.
        logger : logging.Logger, optional
            Where to say what was fetched.

        Returns
        -------
        dict
            ``{batch: [(name, ephemeris), ...]}``, with the pulsars of a batch in the order
            that they were listed (those of a directory in the order of their paths).

        Raises
        ------
        StrategyError
            If an entry is not understood, a name is used twice, a file or directory cannot be
            read, or an ephemeris cannot be fetched.
        """
        entries = spec["pulsars"]
        if not isinstance(entries, list):
            raise StrategyError("'pulsars' must be a list.")
        directory = spec.get("ephemeris directory", DIRECTORY)
        batches, seen = {}, {}
        for entry in entries:
            for name, path, batch in self._entry(entry, directory, fetch, logger):
                for label, text in (("pulsar", name), ("batch", batch)):
                    self._check_name(label, text)
                if name in seen:
                    where = (
                        "twice" if seen[name] == batch else f"in batches {seen[name]} and {batch}"
                    )
                    raise StrategyError(f"The pulsar {name} is listed {where}.")
                seen[name] = batch
                batches.setdefault(batch, []).append((name, path))
        return batches

    @staticmethod
    def _check_name(label, text):
        if not isinstance(text, str) or not _SAFE.match(text):
            raise StrategyError(
                f"The {label} name {text!r} is not usable: it has to start with a letter "
                "or digit and have only letters, digits, and _ . + - after."
            )

    @classmethod
    def _entry(cls, entry, directory, fetch, logger):
        """
        The ``(name, ephemeris, batch)`` of each pulsar that one entry of ``pulsars`` is.

        An entry is a file, a directory (of ``.par`` files, which is each of them), or a mapping
        of ``ephemeris`` (a file or directory), ``batch`` and ``name``, or of ``name`` and
        ``source: atnf``, for a pulsar from the ATNF catalogue.
        """
        if isinstance(entry, str):
            path, batch, name, source = entry, MAIN, None, "file"
        elif isinstance(entry, dict) and set(entry) <= ENTRY_KEYS:
            path, batch, name = entry.get("ephemeris"), entry.get("batch", MAIN), entry.get("name")
            source = entry.get("source", "file")
        else:
            raise StrategyError(
                f"A pulsar is a file name, or a directory of them, or a mapping of ephemeris, "
                f"batch and name, or of name and source, not {entry!r}."
            )
        if source == "atnf":
            if path is not None:
                raise StrategyError(
                    f"A pulsar from the ATNF catalogue has no 'ephemeris': {entry!r}."
                )
            if not isinstance(name, str):
                raise StrategyError(
                    f"A pulsar from the ATNF catalogue needs its 'name': {entry!r}."
                )
            cls._check_name("pulsar", name)  # (it is a file name)
            found = ensure_atnf(name, directory, logger) if fetch else atnf_path(name, directory)
            return [(name, found, batch)]
        if source != "file":
            raise StrategyError(f"The source of a pulsar is 'file' or 'atnf', not {source!r}.")
        if not isinstance(path, str):
            raise StrategyError(f"A pulsar needs its 'ephemeris': {entry!r}.")
        if os.path.isdir(path):
            if name:
                raise StrategyError(
                    f"A name cannot be given to a directory of ephemerides ({path}): "
                    "each is named by its PSRJ."
                )
            return [(pulsar_name(file), file, batch) for file in par_files(path)]
        return [(name or pulsar_name(path), path, batch)]

    # What it makes

    @staticmethod
    def _tail(block, *keys):
        """The settings in a block, copied, for the keys of it that are set."""
        return {key: deepcopy(block[key]) for key in keys if block.get(key) is not None}

    @classmethod
    def _job(cls, block, scheduler=None, htcondor=None):
        """
        The ``scheduler``, ``htcondor`` and ``retry`` settings of a block, copied, with defaults
        for the ones that the block does not give itself (what the block says is not changed).
        """
        found = cls._tail(block, "scheduler", "retry", "htcondor")
        for key, defaults in (("scheduler", scheduler), ("htcondor", htcondor)):
            if defaults:
                found[key] = {**deepcopy(defaults), **found.get(key, {})}
        return found

    @staticmethod
    def _shards(ranges, detectors, spec):
        """
        The shards: ``(number, (first, last), detectors)``.

        With ``split detectors`` there is one shard of each detector for each span, so that data
        that cannot be got for one detector fails only that shard, not the others of the span.
        """
        for number, span in enumerate(ranges, start=1):
            if spec.get("split detectors") and len(detectors) > 1:
                for detector in detectors:
                    yield number, span, [detector]
            else:
                yield number, span, list(detectors)

    def expand(self, blueprint, context):
        spec = blueprint["strategy"]
        run = spec.get("run") or f"{blueprint['name']}-heterodyne"
        detectors = list(spec["interferometers"])
        factors = list(spec.get("frequency factors") or [1, 2])
        joblength = spec.get("joblength", JOBLENGTH)
        heterodyne = _mapping(spec, "heterodyne", HETERODYNE_OPTIONS)
        merge = _mapping(spec, "merge", MERGE_OPTIONS)
        pe = _mapping(spec, "pe", PE_OPTIONS)
        # With no shared filesystem the light jobs (merge, summary) run on the machine that asimov
        # runs on, where the project is, and the parameter estimation has its files transferred.
        shared = spec.get("shared filesystem", True)
        on_this_machine = None if shared else {"universe": "local"}
        transferred = None if shared else {"transfer files": True}
        batches = self.pulsars(spec, fetch=True, logger=context.logger)
        if any(isinstance(e, dict) and e.get("source") == "atnf" for e in spec["pulsars"]):
            context.logger.warning(
                "Some ephemerides are from the ATNF pulsar catalogue, which are not timing "
                "solutions that follow the phase of a pulsar through a run: use them to try the "
                "plugin, not for a result."
            )
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
            for number, (first, last), shard_detectors in self._shards(ranges, detectors, spec):
                suffix = f"-{shard_detectors[0]}" if len(shard_detectors) < len(detectors) else ""
                shard = {
                    "kind": "analysis",
                    "name": f"{prefix}shard-{number:04d}{suffix}",
                    "pipeline": "cwinpy-heterodyne",
                    "event": run,
                    **({"interferometers": shard_detectors} if suffix else {}),
                    **self._tail(heterodyne, "data"),
                    **self._job(heterodyne),
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
                    **self._job(merge, htcondor=on_this_machine),
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
                        **self._tail(pe, "sampler"),
                        **self._job(pe, scheduler=transferred),
                        **({"cwinpy-pe": deepcopy(pe["settings"])} if pe.get("settings") else {}),
                    }
                )
        summary = self.summary(spec)
        if summary is not None:
            documents.append(
                {
                    "kind": "analysis",
                    "name": "summary",
                    "pipeline": "cwinpy-summary",
                    "event": run,
                    "needs": sorted(
                        f"{name}/pe" for members in batches.values() for name, _ in members
                    ),
                    **self._job(summary, htcondor=on_this_machine),
                    **(
                        {"cwinpy-summary": deepcopy(summary["settings"])}
                        if summary.get("settings")
                        else {}
                    ),
                }
            )
        return documents
