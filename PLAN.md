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
5. ~~Failure handling, the rest~~ (done): progress reporting from DAGMan's logs (`while_running` of
   `cwinpy-heterodyne`), `minimum coverage`, and `until_consumed` retention of the per-chunk files.
   (Classification of failures was done in step 4.)
6. ~~Merge pipeline~~ (done): `cwinpy-merge` plans the merges from the shards' files, runs them
   as one job that checks every input first, and provides the merged files for `cwinpy-pe`.
   Tested with the real cwinpy through the whole chain (shards, merge, parameter estimation).
7. ~~`cwinpy-run` strategy~~ (done): one blueprint expands into the run subject, its shards and
   merge, and a subject with a parameter estimation for each pulsar. Pulsars added later are
   a `batch` rather than made by `extend` (see below). The whole chain has been run from one
   blueprint with the real cwinpy (`tests/test_chain_cwinpy.py`).
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
6. That DAGMan's log has the table the progress is read from, in the form the tests write:

   ```
   Of 2000 nodes total:
    Done     Pre   Queued    Post   Ready   Un-Ready   Failed
     ===     ===      ===     ===     ===        ===      ===
     120       0       20       0    1860          0        0
   ```

   (with the date and time at the start of each line), and that it is written often enough to be
   current, and within the last 100 kB of the log. If it is not there the monitor says nothing
   about progress, and nothing else is affected.

## What step 6 taught us

- **cwinpy's `heterodyne_merge` silently drops files that do not exist** and merges the rest, so a
  lost shard file would leave a gap in the data and no failure. The merge job checks every input.
- **Asimov's assets are by name and `_previous_assets()` keeps the last**: an analysis that needs
  several shards, each with `heterodyned files`, sees only one of them. The merge reads each
  dependency's assets itself. Anything else that combines analyses will need to as well.
- **A merge needs a status file of its own.** Unlike the shard's DAG (read from DAGMan's log) and
  the PE job (read from HTCondor's), a job that asimov runs through any scheduler is best asked
  by having it say how it went; and when it has said nothing it is not taken to have failed.
- **Asimov crashes on the YAML ledger when an analysis needs one of another subject** at
  `manage submit` ([etive-io/asimov#321](https://github.com/etive-io/asimov/issues/321)). The
  database ledger, which is the default, is fine, and the chain test uses it.
- The three pipelines fit together: what `cwinpy-heterodyne` provides is what `cwinpy-merge`
  reads, and what that provides is what `cwinpy-pe` reads, checked on real data.

## What the coverage check and retention taught us

- **Coverage is measured, not assumed.** Every output file records the segments of its data, and
  cwinpy writes the segment files it used into `<rundir>/segments` when it makes the DAG from a
  flag ("rather than relying on each job doing it"), so both modes have a science time to compare
  with. On the fake frames a 2048 s job heterodynes 1860 s of 2048 s: the first and last ~90 s are
  lost. A threshold has to allow for that, hence it is a per-analysis setting that is off unless set.
- **A merge is not checked.** It joins what the shards made, and checks that every input is there;
  what was left out is already a fact about the shards.
- **Retention needed one new message, not new behaviour.** Core's `prune` only releases what a
  finished consumer has used and only when asked. After a release a shard's `collect_assets` is empty
  (its files are gone), so a merge that is run again used to say "the shard is not complete"; it now
  says that the files were released, when, and that the shard has to be run again.
- Checked on real data: both chain tests now read the coverage of real shards (0.85 to 0.95), run
  `asimov analysis prune`, find the chunk files gone and the merged files, the posterior and the
  statuses untouched, and `asimov monitor` undisturbed.
- Mutation testing found two pieces of redundant code (a guard that `coverage()` already made, and a
  comment check the number pattern already made), which were removed.

**Not verified, for Matt:** (7) that in flag mode cwinpy's segment files are named
`segments_<start>-<end>_<flag>.txt` with the flag's `:` as `_`, so that `_H1_` finds a detector's (this is
read from cwinpy's code, not seen); (8) that a day of data with no science segments has no job and
no expected output, rather than an empty file, which would stop a shard from completing.

## What progress reporting taught us

- **Where the counts come from.** DAGMan writes a table of how many nodes are done, queued, ready
  and failed to its own log, which is also where the end of the DAG is read. One read of the end of
  that file gives the counts; counting the output files instead would be a few thousand `stat`s of
  a shared file system for each shard on every monitor cycle, and cannot tell a job that is running
  from one that has not started.
- **It is only a report.** `while_running` is called by the monitor while the DAG is in the queue; it
  writes `progress` (the counts and when they were read) to the ledger and one line to the monitor,
  and decides nothing: completion and failure are still read as before. A log with no table, or a
  table cut off, says nothing.
- **A `failed` node is one that has used up its retries**, so a nonzero count here while the DAG runs
  is a job that will be reported when the DAG ends, and the monitor shows it in red.
- The format of the table is not checked against a pool (item 6 above), and the test of it uses the
  form from memory, so it can agree with the code and still be wrong.
- Mutation testing found two tests that did not discriminate (a percentage that happened to be
  exact, and a log long enough to need the tail but with the table in it); both were rewritten.

## What step 7 taught us

- **The plan said `extend` would top up pulsars added later; a `batch` does it better.** `extend`
  runs when an analysis finishes, and what it should add depends on which pulsars are new, which is
  a fact about the blueprint, not about a finished analysis. A blueprint that is expanded again
  (applied again) is the natural moment, but then `expand` must give the same documents for the
  same blueprint, or the group's names would change. So a pulsar is given an explicit `batch`; a
  batch is shards (`<batch>-shard-NNNN`) and a merge (`merge-<batch>`) over the same span, and its
  pulsars' parameter estimations need that merge. Nothing that exists is changed, and the output of
  `expand` is a function of the blueprint alone. The cost is that the user names the batch.
- **A shard is a whole number of jobs.** `shards: {count: n}` is "about n": `ceil(jobs / n)` jobs to
  a shard, so the last is short and fewer than `n` come out if there are fewer jobs than that.
  (A `min(n, jobs)` that I had first was redundant, which a mutation of it showed.)
- **Settings are copied to every shard, not inherited from the run subject.** That keeps each
  analysis complete in the ledger and lets a shard be changed on its own, at the price of a ledger
  that holds the 200 `.par` paths once per shard. Moving `ephemerides` to the run subject would
  need the pipelines to read it from there; not done.
- **Names of pulsars are their `PSRJ`**, read from the `.par` file when the blueprint is applied
  (so the file has to be there). `name:` overrides it. Names are limited to letters, digits and
  `_ . + -`, so that they are safe as directory names.
- **The group cap is core's.** 40 shards and 200 pulsars is about 241 analyses, under 500.
  The strategy does not check it; asimov refuses the blueprint, with a message.
- The `e2e` environment drifts: with Python 3.13 a fresh `pip install cwinpy` brought bilby 2.8.2
  and dynesty 3.2.0, which bilby cannot import (`apply_reflect`); dynesty 3.0.0 works. CI, on 3.11,
  has not been hit by this. Pin if it is.

## Open questions

- Does a heterodyne config for a sub-range of time reproduce the corresponding
  slice of a full-run output? Chunk edges depend on the segment list.
- Does a DAGMan job with failed nodes really appear as Completed on the pool?
- Slurm is out of scope for the DAG path.
- **Report pages.** cwinpy makes summary pages (`cwinpy_generate_summary_pages`, from
  `cwinpy.pe.summary`) for the whole set of pulsars, not per `cwinpy_pe` job: it builds the PE
  pipeline's object from a `cwinpy_knope_pipeline`/`cwinpy_pe_pipeline` configuration
  (`pe_pipeline(config, build=False)`) and reads its `resultsbase` and `resultsfiles`. Asimov's run
  report links a pipeline's pages through `Pipeline.result_pages()` (a list of `(label, url)` relative
  to the report root, shown once the analysis is finished). This plugin does neither yet: nothing is
  generated, and `result_pages()` returns nothing. A `cwinpy-summary` analysis that needs every PE of a
  run, writes such a configuration pointing at their results, runs the generator into the web
  directory and returns its page from `result_pages()` would do both. Not tried: whether
  `pe_pipeline(build=False)` accepts a configuration that was written to describe results made by
  separate `cwinpy_pe` jobs.
