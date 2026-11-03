# asimov-cwinpy

An [asimov](https://github.com/etive-io/asimov) plugin to run the
[cwinpy](https://cwinpy.readthedocs.io) known-pulsar analysis: heterodyning the
data of an observing run for many pulsars, merging the result, and estimating the
parameters of the signal of each pulsar.

> **Status: pre-alpha.** `cwinpy-pe` and `cwinpy-heterodyne` work: each has been run
> through asimov with the real cwinpy (heterodyne on fake frames, with the jobs run
> without HTCondor). `cwinpy-merge` and the `cwinpy-run` strategy are skeletons that
> asimov finds but that cannot build or submit anything yet. Nothing has been run on an
> HTCondor pool. [`PLAN.md`](PLAN.md) says what is done and what is next.

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

### `cwinpy-heterodyne`

One analysis is one *shard* of a run: a span of time, for the detectors in `interferometers`,
heterodyned for all the pulsars in a single HTCondor DAG that cwinpy's own runner writes.
The ledger is turned into the one form of configuration that cwinpy accepts in many, an
explicit dictionary for each detector.

| Ledger key | cwinpy |
|---|---|
| `cwinpy-heterodyne: data start`, `data end` | `starttimes`, `endtimes` (GPS) |
| `cwinpy-heterodyne: ephemerides` | `pulsarfiles`: the parameter files of the pulsars |
| `frequency factors` (default 2) | `freqfactors` |
| `data: channels` | `channels` |
| `data: frame types`, `data: segments`, `data: datafind url`, `data: datafind url type`, `cwinpy-heterodyne: segment server` | `frametypes`, `includeflags`, `host`, `urltype`, `segmentserver`: data from a datafind server |
| `data: cache files`, `cwinpy-heterodyne: segment lists` | `framecaches`, `segmentlists`: data in frame caches, with no server |
| `cwinpy-heterodyne: joblength` | `joblength`: seconds of data per job |
| `cwinpy-heterodyne: strict data requirement` (default **true**) | `strictdatarequirement` |
| `scheduler: request memory`, `request disk`, `request cpus`, `accounting group` | the resources of the jobs |

`strict data requirement` is true here, and cwinpy's own default is false. With false, a job
that cannot read a frame file **exits successfully and writes nothing**, so data is lost
without a failure. With true the job fails, the DAG ends with a rescue file, and submitting
the same DAG again runs only the jobs that failed.

cwinpy writes an **empty file** for each output when it builds the DAG, so a file existing
is not a result. The shard is complete when DAGMan has said that the DAG succeeded *and*
every expected output has data. The expected outputs are written down when the DAG is
built, so asking whether the shard is complete does not need cwinpy.

When the DAG fails, the error output of its jobs is read. Jobs that could not read frame
files make a *transient* failure, which asimov's retry policy (`retry:` in the blueprint)
tries again after a delay; any other failure is not transient. A DAG that has not ended is
left alone, and not treated as failed. Submitting a DAG a second time needs
[etive-io/asimov#314](https://github.com/etive-io/asimov/issues/314).

The result is the `heterodyned files` asset, `{pulsar: {detector: {factor: [path, ...]}}}`,
for `cwinpy-merge`.

### `cwinpy-pe`

One analysis is one pulsar (the name of its subject), run as a single `cwinpy_pe` job.

| Ledger key | `cwinpy_pe` option |
|---|---|
| `ephemeris` | `par-file` |
| `interferometers` | `detector` |
| `frequency factors` (1 and/or 2; default 2) | `data-file-1f`, `data-file-2f`, from the `heterodyned data` asset |
| `priors` | `prior`: a bilby prior file that the pipeline writes. Names of parameters are those of `cwinpy_pe` and are not changed. |
| `sampler: sampler`, `sampler: sampler kwargs` | `sampler`, `sampler-kwargs` |
| `scheduler: periodic restart time` | `periodic-restart-time` |
| `scheduler: request cpus`, `request memory`, `request disk`, `accounting group` | the resources of the job |

The data comes from the `heterodyned data` asset of the analysis it `needs`, a mapping
`{pulsar: {detector: {factor: path}}}`. To try a configuration without any, analyse simulated
noise with `cwinpy-pe: fake asd: {H1: 1.0e-24, L1: 1.0e-24}` (and `fake start`, `fake end`,
`fake dt`, `fake seed`); see [`examples/pulsar-simulated.yaml`](examples/pulsar-simulated.yaml),
which runs in seconds.

The result is the `samples` asset, `<outdir>/<name>_result.hdf5`. The analysis is complete
when that file exists, not when its checkpoint files do.

If `cwinpy_pe` stops itself to be restarted (exit code 77, after `periodic restart time`),
the job is submitted again and resumes from its checkpoint. That is read from the HTCondor log,
so it works on HTCondor only. A job that has gone from the scheduler is treated as failed only
if there is evidence of it (its log, or an exception in its error output); otherwise it is left
alone, because the list of jobs may be out of date.

## Requirements

Asimov with analysis types, the retry policy, the rescue fixes, and the ability to submit a
DAG again ([#314](https://github.com/etive-io/asimov/issues/314)): the release after 0.8.1
(or its `main` branch). cwinpy and HTCondor are needed where jobs run, and are
not dependencies of the plugin. The DAG pipeline supports HTCondor only.

```bash
pip install "asimov @ git+https://github.com/etive-io/asimov@main"
pip install -e ".[dev]"
pytest
```

`tests/test_pe_cwinpy.py` and `tests/test_heterodyne_cwinpy.py` run the real cwinpy and are skipped
unless it is installed.
cwinpy needs the GSL development files to build (`libgsl-dev`), and version 1.5.0 cannot read
its configuration files with ConfigArgParse 1.7.6 or later (1.7.5 is the last that works), so:

```bash
pip install cwinpy "ConfigArgParse<1.7.6"
pytest tests/test_pe_cwinpy.py tests/test_heterodyne_cwinpy.py
```

> **Using Claude Code?** The repository carries an
> [`asimov-plugin` skill](.claude/skills/asimov-plugin/SKILL.md) that explains the
> conventions of asimov plugins. It was written for a template this repository
> started from, so the template's example code that it mentions is not here.
