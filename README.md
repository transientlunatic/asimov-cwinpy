# asimov-cwinpy

An [asimov](https://github.com/etive-io/asimov) plugin to run the
[cwinpy](https://cwinpy.readthedocs.io) known-pulsar analysis: heterodyning the
data of an observing run for many pulsars, merging the result, and estimating the
parameters of the signal of each pulsar.

> **Status: pre-alpha.** `cwinpy-heterodyne`, `cwinpy-merge`, `cwinpy-pe` and `cwinpy-summary` work,
> and have been run through asimov together with the real cwinpy: two shards of fake frames, merged,
> analysed, and summarised (the shards' DAGs are run without HTCondor), from a blueprint that the
> `cwinpy-run` strategy expands. Nothing has been run on an HTCondor pool. [`PLAN.md`](PLAN.md) says what is done and
> what is next.

## What it provides

| Name | Kind | What it does |
|---|---|---|
| `cwinpy-heterodyne` | pipeline | Heterodynes one *shard* (a span of time) of an observing run for all of its pulsars, as one HTCondor DAG made by cwinpy. |
| `cwinpy-merge` | pipeline | Merges the heterodyned files of the shards of a run, for each pulsar, detector and frequency factor, into one file each. |
| `cwinpy-pe` | pipeline | Estimates the parameters of the signal of one pulsar, as a single `cwinpy_pe` job. |
| `cwinpy-summary` | pipeline | Makes cwinpy's summary pages for the parameter estimations of a run, and links them from asimov's report. |
| `cwinpy` | analysis type | A generic analysis, with no gravitational-wave transient metadata. The pipelines above declare it. |
| `cwinpy-run` | strategy | Expands one description of a run into its shards, merge, and a subject with a parameter estimation for each pulsar. |

## How a run is laid out

```
O4a-heterodyne            (a "run" subject)
  shard-0001 … shard-00NN   cwinpy-heterodyne
  merge                     cwinpy-merge        needs all the shards
  summary                   cwinpy-summary      needs the pe of every pulsar (optional)
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

### One blueprint for a whole run

The `cwinpy-run` strategy writes all of that from one blueprint
([`examples/whole-run.yaml`](examples/whole-run.yaml)):

```yaml
kind: analysis
name: O4a
strategy:
  type: cwinpy-run
  interferometers: [H1, L1]
  data start: 1368975618
  data end: 1380000000
  shards: {count: 40}            # or {length: seconds}
  pulsars: [/path/to/timing-solutions, {name: J0534+2200, source: atnf}]   # a directory; a name
  heterodyne: {data: {...}, retry: {attempts: 10, delay: 3600}}
```

| Option | |
|---|---|
| `interferometers`, `data start`, `data end`, `shards`, `pulsars` | Required. `frequency factors` defaults to `[1, 2]`; `run` names the run subject (default `<name>-heterodyne`); `joblength` is the length of data one job reads (default a day). |
| `ephemeris directory` | Where ephemerides that are fetched are put (`ephemerides`, in the project, unless it says). |
| `shared filesystem` | `false` if the execute nodes cannot see the project: see [Clusters whose nodes do not see the project](#clusters-whose-nodes-do-not-see-the-project). Default `true`. |
| `split detectors` | `true` for one shard of each detector for each span of time, instead of one shard of all the detectors. Default `false`. See [Splitting shards by detector](#splitting-shards-by-detector). |
| `heterodyne` | `data`, `scheduler` and `retry` go on every shard as they are; `settings` goes under `cwinpy-heterodyne:` (`strict data requirement`, `segment lists`, ...). |
| `merge` | `scheduler`, `retry`. |
| `pe` | `sampler`, `scheduler`, `retry` go on each parameter estimation; `priors` on each pulsar; `settings` under `cwinpy-pe:`. |
| `summary` | Optional. `true`, or a mapping of `settings` (under `cwinpy-summary:`), `scheduler` and `retry`: adds the `summary` analysis, which needs the parameter estimation of every pulsar. |

A shard is a whole number of jobs, so `shards: {count: 40}` makes about 40 (fewer if there are
fewer jobs), with the last one short. Every name comes from the blueprint alone, so applying it
again changes nothing that exists, and an error applies nothing.

**Where the ephemerides come from.** An entry of `pulsars` is:

| Entry | Pulsars |
|---|---|
| `/path/to/J0534+2200.par` | that file, named by its `PSRJ` (or `PSR`, or `NAME`) |
| `/path/to/directory` | every `.par` file in it and under it, in the order of their paths, each named by its `PSRJ` |
| `{ephemeris: <file or directory>, batch: late, name: ...}` | the same, in a batch; `name` overrides the name of a file (not of a directory) |
| `{name: J0534+2200, source: atnf}` | the pulsar's entry in the ATNF pulsar catalogue, fetched once with `psrqpy` |

A pulsar from the catalogue is written to `<ephemeris directory>/<name>.par`, with
`<name>.atnf.json` that says which version of the catalogue and of `psrqpy` it was, and then used
from there: **a file that is already there is used and not fetched**, so applying the blueprint again
needs no network, and a timing solution of your own put there first is not replaced. The options are
checked without the network; only applying fetches, and a pulsar that is not in the catalogue stops it
before anything is applied. **The catalogue's ephemerides are not for a result**: they say where a
pulsar is, and are not timing solutions that follow its phase through a run, so asimov warns when
they are used. Use them to try the plugin. For a run, give the timing solutions as files or a
directory. Do not add files to a directory that is already in a blueprint to add pulsars: what was
listed before is not changed, so they would be given no data (use a batch).

**Pulsars added later** are given a `batch` (`{ephemeris: ..., batch: late}`). A batch has shards
(`late-shard-0001` ...) and a merge (`merge-late`) of its own, over the same span, so the data of
the pulsars that are done is not made again, and their analyses are not changed. A pulsar's
parameter estimation needs the merge of its batch. The strategy counts about
`shards x batches + pulsars + batches` analyses; asimov refuses a group of more than
`[strategy] max_group_analyses` (500 unless set).

### Splitting shards by detector

cwinpy makes the DAG of a shard only if every detector has some science data in the whole span of
it, and a job that cannot read its data fails the shard. With the detectors in one shard, data that
is missing for one detector (a stretch with no segments, a frame that cannot be got) fails the
analysis of the others too. With `split detectors: true` there is a shard for each detector for each
span (`shard-0007-H1`, `shard-0007-L1`), each with `interferometers` of its own in the ledger, and the
same span and settings otherwise. A failure then costs one detector for one span, and
the retries of asimov are for that detector alone. The merge needs all of them, and joins the files of
each detector as it did, so what the parameter estimation reads does not change. A shard that was
given up on is left out of the merge, as before, so the other detector of its span is still used.

The price is twice as many shards for two detectors (40 time shards are 80 analyses, and the limit of
asimov for a group is 500), and twice the DAGs. With one detector nothing is split. It is off by default so that the
names of the analyses of a project that exists do not change; for a run of any length it is the better choice.

### Clusters whose nodes do not see the project

On some clusters the execute nodes do not see the project, nor the home directory that it (and the
environment of a user) may be in, and the pool does not allow `getenv`. Two things are then needed:
software that the nodes can run, and the jobs that asimov describes made independent of the project.

**The software.** Every job runs from a software environment: a directory with `bin/` in it. The
default is `[pipelines] environment` in the configuration of the project; `scheduler: environment` on
an analysis replaces it for that analysis. Use one that the nodes see, such as an IGWN environment on
CVMFS that has cwinpy. It is where cwinpy finds the executables that it writes into the DAG of the
heterodyne, and where `cwinpy_pe` is run from. Two things follow. The DAG is built by the cwinpy that
asimov runs in and is run by the cwinpy of the environment, so they should be the same version.
And the DAG files record the path: changing the environment needs the DAG to be made again (a new
project, or remove the `submit/` directory and `manifest.json` of the shard).

**The jobs.** Set `shared filesystem: false` in the strategy:

| Stage | What it does |
|---|---|
| `cwinpy-heterodyne` | nothing: cwinpy's DAG already transfers its own inputs and outputs, and does not use `getenv` |
| `cwinpy-merge`, `cwinpy-summary` | run as `htcondor: universe: local` jobs, on the machine that asimov runs on, where the project is, with the Python that asimov runs in (which therefore needs this package and cwinpy). They are light. |
| `cwinpy-pe` | `scheduler: transfer files: true`: HTCondor copies the configuration, prior, ephemeris and heterodyned data to the node, and the `results` directory comes back to the run directory. |

For `cwinpy-pe` every path in the configuration is then the name of a file in the directory of the job
(`par-file = J0534+2200.par`, `outdir = results`). The directory of the results is sent with the job
from the start, so a job that stops itself to be started again (`periodic restart time`, which ends with
the exit code 77 that asimov takes for a restart) gets its checkpoint back, and so that HTCondor never has an
output that was not made, which it would put on hold. The files that are to be sent are checked to be
there before the job is submitted, for the same reason. None of the jobs asks for `getenv`.

What a job needs of its environment, besides what is in the software environment, is said with
`scheduler: environment variables` (a mapping, which becomes HTCondor's `environment`), and the machines
that may run it with `htcondor: requirements` (also given to the jobs of the heterodyne DAG). A pool
that does allow it can have `htcondor: getenv: true`.

These are all set on the blocks of the strategy as well (`heterodyne`, `merge`, `pe`, `summary`), and
what they say is kept: `shared filesystem: false` only gives the defaults above.

```yaml
strategy:
  type: cwinpy-run
  shared filesystem: false
  heterodyne: {scheduler: {environment: /cvmfs/software.igwn.org/conda/envs/igwn}}
  pe: {scheduler: {environment: /cvmfs/software.igwn.org/conda/envs/igwn}}
  # ...
```

The real `cwinpy_pe` has been run through an emulation of HTCondor's file transfer (with the project
hidden while the job runs, and nothing inherited from the environment): it analyses, stops for a restart,
is sent its checkpoint, resumes and finishes, and its results come back (`tests/test_transfer_cwinpy.py`).
It has not been run on a pool, which is where the transfer of a directory both ways, `initialdir`
and the local universe are still to be confirmed.

### Trying it at a smaller scale

How long a run takes is set by reading the frames and by the parameter estimations, and neither
has been timed on a pool yet, so the sizes below are in what they read and run, not in hours.

| To test | Use | What it reads and runs |
|---|---|---|
| The code, with no pool | `pytest tests/test_chain_cwinpy.py` (needs cwinpy) | fake frames, 2 shards, a merge, a PE: about 1.5 minutes |
| The parameter estimation alone, on a pool | [`examples/pulsar-simulated.yaml`](examples/pulsar-simulated.yaml) | no data at all: simulated noise |
| The whole chain, small | [`examples/test-run.yaml`](examples/test-run.yaml) | one day of data, two pulsars (by name, from the ATNF catalogue: nothing to fill in but the project), one frequency factor, 6-hour jobs: 8 heterodyne jobs, 1 merge, 2 PEs |
| A whole run | [`examples/whole-run.yaml`](examples/whole-run.yaml) | the span of the run: days x detectors x factors heterodyne jobs, and a PE for each pulsar |

To make a run smaller, change the blueprint and nothing else: a shorter `data start` to `data end`
(the frame reading is the same however it is divided), fewer `pulsars` (each adds a PE, and a little
to every heterodyne job), `frequency factors: [2]`, a lower `nlive` in `pe: sampler: sampler kwargs`,
and a short `retry: {delay: 60}` so that the retry policy can be seen working.

**Choosing the span.** cwinpy makes the DAG of a shard only if *every* detector has some science
data in the whole span of the shard, and otherwise stops with `No science data segments exist for H1`
(the plugin adds which shard, span and flag, and what to do). So a span in which one detector was down
for all of a shard fails that shard, even if the other detector observed throughout, and the more
and shorter the shards, the likelier it is. For a test, choose a day on which both were observing
(`test-run.yaml` is one shard for that reason). With access to the segment database, for example:

```python
from gwpy.segments import DataQualityFlag

start, end = 1368975618, 1368975618 + 30 * 86400
h1 = DataQualityFlag.query("H1:DMT-ANALYSIS_READY:1", start, end)
l1 = DataQualityFlag.query("L1:DMT-ANALYSIS_READY:1", start, end)
both = h1.active & l1.active
for day in range(start, end, 86400):
    hours = sum(min(b, day + 86400) - max(a, day) for a, b in both if b > day and a < day + 86400)
    print(day, f"{hours / 3600:4.1f} h with both detectors")
```

Take a day with most of its hours, and put its GPS time in `data start` and `data end`. (A shard with
no science data for a detector is not skipped or run without it: that is a decision the plugin does not
make for you yet; see `PLAN.md`.)

Asimov itself is not the limit at the full size. A blueprint of 200 pulsars in 40 shards, made into
233 analyses in 201 subjects, took 7 s to apply, 9 s for `asimov monitor` and 11 s for `asimov manage
build`, on a scratch project (a ledger of 1.2 MB); that is under the default limit of 500 analyses
in a group. That says nothing of how long the jobs take.

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

| Ledger key (any of the four analyses) | |
|---|---|
| `scheduler: environment` | the software environment of the analysis, instead of `[pipelines] environment` |
| `scheduler: environment variables` | variables that are set in the job (as `environment`) |
| `scheduler: transfer files` | `cwinpy-pe` only: have HTCondor transfer the files of the job |
| `htcondor: universe` | `local` to run the job (merge, summary) on the machine that asimov runs on |
| `htcondor: requirements` | the requirements of the jobs (a ClassAd expression) |
| `htcondor: getenv` | `true` to give the jobs the environment of asimov (off unless set) |

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
| `cwinpy-heterodyne: minimum coverage` (optional) | not cwinpy's: the fraction of the science time that has to be heterodyned |
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

**Coverage.** When a shard finishes, the share of its science time that was heterodyned is
measured for each detector and kept in the ledger as `coverage` (`science` and `heterodyned` in
seconds, and the `fraction`). *Heterodyned* is the time of the segments that cwinpy records in its
output files; *science time* is that of the segments the data was meant to come from, inside the
span of the shard: the `segment lists` files, or, when the data is found from a flag, the files that
cwinpy writes to `<rundir>/segments` when it makes the DAG. With `minimum coverage` set, a shard that
heterodyned less than that is not complete: the failure (not transient) is handed to the `retry`
policy like any other. If the science time cannot be found or an output cannot be read, nothing is
checked, and a warning says so. The heterodyne loses a little at the ends of each stretch of data (about
90 s each in a 2048 s test job, so 0.91 there), so a short job needs a lower minimum than a long one.
One pulsar's files, for one frequency factor, are measured, as every pulsar has the same data.

**Releasing the per-chunk files.** The shard's `heterodyned files` are marked `until_consumed`, so
`asimov analysis prune` can remove them once `cwinpy-merge` (and anything else that needs the shard) has
finished, which for a run of 200 pulsars is most of what it writes. They are only removed when you ask,
and the merged files, and the results of the parameter estimations, are never touched. Merging again
after that needs the shard to be run again (asimov says so).

While the DAG runs, `asimov monitor` shows how far it has got, read from DAGMan's own log:
`120 of 2000 jobs done (6%), 20 queued or running, 3 failed` (a job counts as failed when it has
used up its retries). The counts are kept in the ledger as `progress`, with when they were read.
It only reports: whether the shard is complete or has failed is decided as above. The format of
DAGMan's table has not been checked on a pool; if it is not found nothing is shown.

The result is the `heterodyned files` asset, `{pulsar: {detector: {factor: [path, ...]}}}`,
for `cwinpy-merge`.

### `cwinpy-merge`

One analysis merges the output of all the shards of a run, so it `needs` them:

```yaml
kind: analysis
name: merge
pipeline: cwinpy-merge
needs: [shard-0001, shard-0002]
```

It has no settings of its own. Its configuration, which asimov makes when every shard is
complete, is the *plan* of the merge as JSON: for each pulsar, detector and frequency factor, which
files of which shards make which merged file. One job (`python -m asimov_cwinpy.merge_job`) makes
all the merges, because starting cwinpy takes longer than a merge does. It needs this package, as
well as cwinpy, in the environment it runs in.

* Every shard it `needs` must be complete. One that is not is an error, since merging without it
  would leave a gap in the data without saying so. A shard that asimov gave up on (`on failure:
  skip`) is left out, with a warning.
* cwinpy's own `heterodyne_merge` **silently drops any input file that does not exist** and merges
  what is left. The job checks every input first: a missing file stops everything, and is a
  transient failure (a disk may have gone); an empty one stops everything, and is not.
* The job writes a status file, which is how the pipeline knows how it went on any scheduler.
  The merge is complete when it says it succeeded *and* every merged file has data. A job that
  has gone without saying anything is left alone, not taken to have failed.

The result is the `heterodyned data` asset, `{pulsar: {detector: {factor: path}}}`, which
`cwinpy-pe` reads.

### `cwinpy-summary`

cwinpy makes pages of results for a set of pulsars (`cwinpy_generate_summary_pages`): a table of
upper limits, and for each pulsar its posteriors and data. It makes them for the *set*, from the
configuration of cwinpy's own pipeline, which finds its results and data by the names and
directories that cwinpy gives them. The `cwinpy_pe` jobs here are separate and write where asimov
says, so this analysis, which needs the `pe` of every pulsar of a run (and so is built when they have
all finished), *stages* what cwinpy looks for in its run directory and writes the configuration that
points at it:

| Staged | As |
|---|---|
| `par/<pulsar>.par`, `data/<detector>_<factor>f_<pulsar>.hdf5` | hard links to the ephemeris and the merged heterodyned data (copies if the file system cannot link), because cwinpy refuses a symbolic link as an input |
| `results/<pulsar>/cwinpy_pe_<detectors>_<pulsar>_result.hdf5` | a symbolic link to the result |

One job (`python -m asimov_cwinpy.summary_job`) then makes the pages, and says how it went in a status
file, as the merge does. When it has succeeded the pages are copied to the project's web directory, in
`<webroot>/<run subject>/<analysis>/` (so, `.../O4a-heterodyne/summary/html/home.html`), and
`result_pages()` gives asimov's report that link, which it shows for the finished analysis. The pages
use relative links, so they can be moved.

| Ledger key (under `cwinpy-summary:`) | `generate_summary_pages` |
|---|---|
| `posteriors`, `individual posteriors`, `time series`, `upper limit plot`, `odds plot` | `showposteriors`, `showindividualposteriors`, `showtimeseries`, `upperlimitplot`, `oddsplot` |
| `snr`, `odds` | `showsnr`, `showodds` |
| `sort by`, `sort descending`, `only joint`, `only msps` | `sortby`, `sortdescending`, `onlyjoint`, `onlymsps` |

What is not set is left to cwinpy's default. The pulsars of one summary have to share their
detectors and frequency factors (cwinpy takes one set for all of them), so analyses with different ones
need separate summaries; a pulsar whose parameter estimation used simulated data cannot be in it, as
there is no heterodyned data to show. `cweqgen`, which cwinpy needs for the table of upper limits, and `setuptools<82` (cwinpy
imports `pkg_resources` to copy the style sheets, and `setuptools` 82 and later do not have it) have to
be installed where the job runs; the job says so if either is missing. The plots of the time series and the upper limits read the heterodyned
data, and cwinpy cannot make them from a very short stretch of it (32 points, as in the tests): switch
them off with `time series: false` and `upper limit plot: false` for a test of that size.

A batch of pulsars added later is not added to a `summary` that exists, because what exists is not
changed: its `needs` are those of the blueprint when it was first applied. Make another summary of them,
or run the analysis again with the new `needs`.

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
pip install cwinpy "ConfigArgParse<1.7.6" cweqgen "setuptools<82"
pytest tests/test_pe_cwinpy.py tests/test_heterodyne_cwinpy.py tests/test_chain_cwinpy.py
```

> **Using Claude Code?** The repository carries an
> [`asimov-plugin` skill](.claude/skills/asimov-plugin/SKILL.md) that explains the
> conventions of asimov plugins. It was written for a template this repository
> started from, so the template's example code that it mentions is not here.
