# Plan: an asimov plugin for cwinpy

Status: planning. No plugin code has been written. This repository is still the
unmodified plugin template.

## Goal

Run the cwinpy known-pulsar analysis under asimov: heterodyne a whole observing
run for ~200 pulsars, then run parameter estimation for each pulsar, with
automatic monitoring and resubmission of failed heterodyne jobs.

## What was checked in asimov (a spike, not assumptions)

- A pulsar works as a subject: a subject named `J0534+2200` with arbitrary
  metadata (`ephemeris`, `interferometers`, `run`) applies, builds and gates its
  analyses. No GPS time or other GW-transient fields are needed.
- Dependencies resolve within a subject or as `subject/name`. A project analysis
  is not an analysis of a subject, so a per-pulsar analysis cannot `need` one.
  The heterodyne stage therefore lives under a "run" subject.
- A subject `O4a-heterodyne` holding shards and a merge applied fine, and a
  pulsar analysis with `needs: ["O4a-heterodyne/merge"]` resolved to it.
- Older asimov monitors ignore cross-subject `needs`, so the plugin must pin a
  recent asimov.
- Pulsar analyses were `GravitationalWaveTransient` objects with empty GW
  defaults; analysis types (asimov #302) fix that, and the plugin uses one.

## What was read in cwinpy

- `HeterodyneDAGRunner` fills `heterodyned_files[det][ff][psr]` and
  `mergeoutputs` before writing the DAG, so the expected outputs are known up
  front. `submitdag` defaults to false, so asimov can submit the `.dag`.
- Its `eval` helper uses `simpleeval`.
- Building the DAG queries data and segments, so it must run where that access
  exists.
- A heterodyne job reads one chunk of data once and writes output for every
  pulsar, which is why heterodyning cannot be per pulsar.
- For one pulsar, PE is a single `cwinpy_pe --config` job, so `PEDAGRunner` is
  not needed.
- Not tested here: cwinpy and HTCondor could not be installed in the
  development container. Everything touching them needs testing on a real pool.

## Architecture

```
O4a-heterodyne (run subject)
  shard-0001 … shard-00NN   cwinpy-heterodyne   one DAG each, all pulsars
  merge                     cwinpy-merge        needs all shards
J0534+2200 … (~200 pulsar subjects)
  pe                        cwinpy-pe           needs O4a-heterodyne/merge
```

- `cwinpy-heterodyne`: writes one canonical config per shard (explicit
  per-detector dicts, a list of par files, explicit times), calls the runner with
  `merge=False` and a per-shard output directory, submits the DAG through the
  scheduler. Complete when every expected output file exists (a cheap check; it
  runs every monitor cycle).
- `cwinpy-merge`: runs `cwinpy_heterodyne_merge`. Asset:
  `{pulsar: {detector: {freqfactor: path}}}`.
- `cwinpy-pe`: one `cwinpy_pe` job per pulsar, reading its slice of the merge
  asset. Priors via asimov's `PriorInterface`, converted to cwinpy's format.
- `cwinpy-run` strategy: expands a run description (run name, detectors, GPS
  span, shard size by length or by count, par-file list) into the run subject,
  shards, merge, and a subject plus PE per pulsar. Names must be deterministic.
  About 300 analyses is under the default group cap of 500 (configurable).
  `extend` can create a top-up heterodyne for pulsars added later.

Analyses use the `cwinpy` analysis type (`analysis_type` on the pipelines), so a
pulsar analysis is a generic one and has no gravitational-wave metadata or PSD
lookup. The type also carries the vocabulary terms the pipelines share
(`ephemeris`, `frequency factors`).

Shard size is deliberately a parameter: 40–100 shards is the working estimate,
not a constraint.

## Failure handling (the main motivation)

A single unreadable frame file fails a heterodyne job, and a monolithic DAG
cannot be resubmitted until everything else has finished. Sharding makes a
failed shard finish early; DAGMan resumes from the rescue file when the same
`.dag` is resubmitted, so only the failed nodes rerun.

Asimov core now owns the retry policy
([#240](https://github.com/etive-io/asimov/issues/240), implemented in
[#299](https://github.com/etive-io/asimov/pull/299)): a `retry` block on an
analysis (or a pipeline default) sets the number of attempts and the delay and
backoff between them, with fewer attempts for a deterministic failure; each
failure is recorded; after the last, `on failure: halt` leaves the analysis
`stuck` and `skip` gives it up so that what needs it goes ahead. So the plugin
does **not** need its own back-off or attempt counting. What it provides is the
mechanism and the classification:

- `resurrect()` is called when a job has left the scheduler incomplete. It
  inspects the DAG's node logs and raises `TransientCwinpyError` for a
  frame-read failure, or `CwinpyError` for anything else. Asimov reads the
  `transient` attribute to decide how many times to try again.
- A retry resubmits through the normal `submit_dag()`, so `submit_dag()` must
  resume: if the DAG has been written and has a rescue file, resubmit it
  rather than building it again.
- `while_running` reports node counts from DAGMan's logs.
- A coverage check fails a shard or merge if the fraction of data ignored
  exceeds a threshold (the first O4 run silently ignored too much data).
- `output_paths()` and `output_retention` (asimov
  [#300](https://github.com/etive-io/asimov/pull/300)) let the per-chunk
  heterodyned files be released once the merge has consumed them.

The `TransientCwinpyError` / `CwinpyError` pair is already in `asimov_cwinpy/common.py`.

## Changes needed in asimov core

Done, in [PR 298](https://github.com/etive-io/asimov/pull/298) (merged):

1. [#295](https://github.com/etive-io/asimov/issues/295): save the ledger after a
   successful `resurrect()`, otherwise a resubmission's job id is lost.
2. [#296](https://github.com/etive-io/asimov/issues/296): do not mark a job as
   finished merely because the scheduler calls it completed; ask
   `detect_completion()`. A DAG with failed nodes still ends as a completed
   DAGMan job (to confirm on a real pool).
3. [#297](https://github.com/etive-io/asimov/issues/297): document the contract
   of `resurrect()`.

Since then asimov main has gained the retry policy (#299), output retention
(#300) and analysis and subject types (#302–#304), all of which the plugin uses.
They are newer than the 0.8.1 release, so the plugin needs asimov from `main`
until the next release; CI installs it from there.

The retry policy's keys were not in the ledger vocabulary at first, so
`asimov vocabulary check` rejected them; that is fixed
([#311](https://github.com/etive-io/asimov/issues/311), #312).

## Work, in order

1. ~~Core fixes~~ (done, #298).
2. ~~Scaffold~~ (done): the package is renamed, the three pipelines, the
   `cwinpy` analysis type, the `cwinpy-run` strategy and the vocabulary are
   registered as skeletons, with tests, examples and CI.
3. ~~PE pipeline~~ (done): `cwinpy-pe` builds, submits and recognises completion, and has
   been run through asimov with the real `cwinpy_pe` on simulated noise (`tests/test_pe_cwinpy.py`,
   and a CI job). Not done: the `bayes factors` asset from the result file.
4. ~~Heterodyne shard pipeline~~ (done, except as listed under open questions): `cwinpy-heterodyne`
   turns the ledger into one canonical configuration, has cwinpy's own runner build the DAG, records
   the files to expect, submits it (and submits it again to resume it), recognises completion, and
   classifies failures. Tested with the real runner and the real `cwinpy_heterodyne` jobs on fake
   frames; no HTCondor.
5. Failure handling, the rest: progress reporting from DAGMan's logs, the coverage threshold,
   and output retention. (Classification of failures is done, in step 4.)
6. Merge pipeline and assets.
7. `cwinpy-run` strategy.
8. End-to-end: a mock run with fake `cwinpy` executables and injected frame
   failures to prove the retry loop; O1 hardware injections on LDG; a small O4
   slice.
9. Documentation and release.

## What step 3 taught us

- **A job that has gone is not necessarily dead.** Asimov's list of jobs is cached for 15
  minutes, and the local scheduler cannot be asked from another process, so a job that was
  just submitted can look gone. A `resurrect()` that raises on that marks a healthy job
  `stuck`. The end-to-end test found this. `resurrect()` raises only on evidence of failure
  (the job's log, or an exception in its error output), as the command pipeline's does, and
  otherwise leaves the job alone. The heterodyne pipeline must do the same.
- **The HTCondor log is a sequence of events.** After a restart it still holds the end of the
  first run until the job is submitted again, so the last event is what counts.
- **cwinpy 1.5.0 and ConfigArgParse 1.7.6 or later are incompatible.** The configuration file
  cannot be read at all (`AttributeError` in `CWInPyConfigFileParser.parse`). 1.7.5 is the last
  version that works, found by trying each release. Worth telling the cwinpy developers; CI pins
  ConfigArgParse below 1.7.6. (A first guess of "1.9" was wrong: CI installed 1.7.7.)
- **cwinpy builds in a clean environment** given the GSL development files, so the real
  `cwinpy_heterodyne` can be exercised in tests for step 4 too.

## What step 4 taught us

Found by running cwinpy, not from its documentation:

- **cwinpy writes an empty file for each output when it builds the DAG.** A file that exists is
  not a result. Completion is "DAGMan said the DAG succeeded and every expected output has data".
- **With cwinpy's default, an unreadable frame does not fail a job.** The job exits 0 and writes
  nothing (`strictdatarequirement` false). That is how the first O4 run lost data without anyone
  knowing. The plugin defaults to true, and reports a DAG that succeeded with empty outputs.
- **What a job writes when it cannot read a frame** (made by damaging one): `OSError: Failed to
  read frame data from cache`, and `XLAL Error - XLALFrFileOpenURL ... Could not open TOC for
  frame file` / `I/O error`. These are the signatures `FRAME_READ_ERRORS` looks for.
- **Every node already has `RETRY 2`**, which retries at once. That is the retry that fails for
  the same cause as the first try. The retry that waits is asimov's.
- **cwinpy submits its own DAGs with `force`**, and asimov's scheduler could not. Without it,
  `condor_submit_dag` refuses to run a DAG again while the files of its last run exist, so a retry
  could not work. [etive-io/asimov#314](https://github.com/etive-io/asimov/issues/314), fixed in
  [#315](https://github.com/etive-io/asimov/pull/315).
- **The DAG can be built offline** from frame caches and segment lists, which is what makes the
  tests possible. A frame cache can be a directory of frames.
- **Checking a shard costs about one `stat` per expected file**, once DAGMan has finished and not
  before: a shard of a few dozen jobs for 200 pulsars has a few thousand expected files.

**Not verified, because there is no HTCondor here, and for Matt to check on a pool:**

1. That DAGMan's log ends with `EXITING WITH STATUS n`, which is how the end of the DAG is read.
   (This is from memory of DAGMan; the tests write that line themselves.)
2. That submitting the same DAG file again with `force` resumes from the rescue file, and that
   `condor_submit_dag` then removes the files it complains about and nothing else.
3. That a DAGMan job whose nodes failed shows as Completed to asimov (asimov #296 handles it).
4. That a real unreadable disk gives the signatures above, and not only a truncated file does.
5. That the per-node `.err` files are where `failed_nodes` looks (`<rundir>/log/`), after a
   retry has overwritten them.

## Open questions

- Does a heterodyne config for a sub-range of time reproduce the corresponding
  slice of a full-run output? Chunk edges depend on the segment list.
- Does a DAGMan job with failed nodes really appear as Completed on the pool?
- Slurm is out of scope for the DAG path.
