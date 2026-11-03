# asimov-cwinpy

An [asimov](https://github.com/etive-io/asimov) plugin to run the
[cwinpy](https://cwinpy.readthedocs.io) known-pulsar analysis: heterodyning the
data of an observing run for many pulsars, merging the result, and estimating the
parameters of the signal of each pulsar.

> **Status: pre-alpha.** The plugin is a skeleton. Asimov finds the pipelines, the
> analysis type and the strategy, and the example blueprints can be applied and
> checked, but nothing can be built or submitted yet. [`PLAN.md`](PLAN.md) says what
> is done and what is next.

## What it provides

| Name | Kind | What it does |
|---|---|---|
| `cwinpy-heterodyne` | pipeline | Heterodynes one *shard* (a span of time) of an observing run for all of its pulsars, as one HTCondor DAG made by cwinpy. |
| `cwinpy-merge` | pipeline | Merges the heterodyned files of the shards of a run, for each pulsar, detector and frequency factor. |
| `cwinpy-pe` | pipeline | Estimates the parameters of the signal of one pulsar, as a single `cwinpy_pe` job. |
| `cwinpy` | analysis type | A generic analysis, with no gravitational-wave transient metadata. The pipelines above declare it. |
| `cwinpy-run` | strategy | Will expand a description of a run into its shards, merge, and one analysis per pulsar. |

## How a run is laid out

```
O4a-heterodyne            (a "run" subject)
  shard-0001 … shard-00NN   cwinpy-heterodyne
  merge                     cwinpy-merge        needs all the shards
J0534+2200 …              (one subject per pulsar)
  pe                        cwinpy-pe           needs O4a-heterodyne/merge
```

A heterodyne job reads a span of data once and writes output for every pulsar, so
heterodyning is done for the run, not for each pulsar. Estimating the parameters is
done for each pulsar, and needs the merge of the run through `subject/name`.

A frame file that cannot be read fails a job, and a DAG cannot be resumed until it
has finished. Splitting a run into shards means a shard with a failure finishes
early and can be tried again, using asimov's own
[retry policy](https://github.com/etive-io/asimov/pull/299) (`retry:` and `on failure:`
in a blueprint). The plugin will tell the policy which failures are transient.

See [`examples/`](examples) for blueprints, and check them with:

```bash
asimov vocabulary check examples/*.yaml
```

## Ledger keys

Standard asimov keys keep their names (`interferometers`, `needs`,
`scheduler: request memory`, …). Shared by the three pipelines, and registered on the
`cwinpy` analysis type:

* `ephemeris`: the path to the pulsar's Tempo(2)-style `.par` file.
* `frequency factors`: the factors of the rotation frequency to heterodyne at, e.g. `[1, 2]`.

Only `cwinpy-heterodyne` has settings of its own so far, under `cwinpy-heterodyne:`:
`data start` and `data end` (GPS times) and `joblength` (seconds of data per job).

## Requirements

Asimov with analysis types, the retry policy and the rescue fixes: the release after
0.8.1 (or its `main` branch). cwinpy and HTCondor are needed where jobs run, and are
not dependencies of the plugin. The DAG pipeline supports HTCondor only.

```bash
pip install "asimov @ git+https://github.com/etive-io/asimov@main"
pip install -e ".[dev]"
pytest
```

> **Using Claude Code?** The repository carries an
> [`asimov-plugin` skill](.claude/skills/asimov-plugin/SKILL.md) that explains the
> conventions of asimov plugins. It was written for a template this repository
> started from, so the template's example code that it mentions is not here.
