---
name: asimov-plugin
description: Use when creating a new Asimov pipeline plugin (a package that lets Asimov build, submit, monitor and collect results from a gravitational-wave analysis code, e.g. "make an asimov plugin for <code>", "wire <pipeline> into asimov", "add asimov support for <code>"). Also useful when reviewing or extending an existing asimov-* plugin repo, or when Asimov's Pipeline/PriorInterface hooks or its Liquid config templating need explaining.
---

# Building an Asimov pipeline plugin

Asimov (https://github.com/transientlunatic/asimov) drives gravitational-wave
parameter-estimation workflows: it builds config files, submits jobs to a
scheduler, monitors them, and collects results. A "plugin" is a small,
separately-published Python package that teaches Asimov how to do this for
one specific analysis code (Bilby, PyCBC, BayesWave, LALInference, Jim, ...),
registered via a `asimov.pipelines` entry point.

This skill distills the process and the real gotchas found while building
[asimov-jim](https://github.com/transientlunatic/asimov-jim) (a plugin for
the JAX-based `jim-run` PE code). **Everything below is demonstrated live in
this template's own `asimov_myplugin/` package** — `pipeline.py` (single-job
submission via the scheduler abstraction, GPU requests, ledger checks in
`before_config`, template helpers such as `render_priors()`), `priors.py`
(type-only prior conversion), `configs/myplugin.ini` (the bundled Liquid
template), `vocabulary.yaml` (the plugin's registered ledger terms),
`examples/` (a blueprint checked in CI) and `tests/` are a complete, working, tested
example, not just prose describing one. Read those files directly; you
shouldn't need to leave this repository to learn the core mechanics.

## 0. Start here: read the template's own example

Clone this template and read, in order: `asimov_myplugin/pipeline.py`,
`asimov_myplugin/priors.py`, `asimov_myplugin/configs/myplugin.ini`,
`asimov_myplugin/vocabulary.yaml`, `tests/test_pipeline.py`. Every pattern
in §§3-8 below is implemented and tested there. Rename the package and go from there:

```bash
git clone https://github.com/etive-io/asimov-plugin-template.git asimov-<name>
cd asimov-<name>
mv asimov_myplugin asimov_<name>
```

`hooks.py` (applicator/postmonitor hooks) is a separate, optional feature —
keep it only if you actually need one; most plugins don't. If you do, the
same ledger rules apply (§5a): record fetched files under the standard
`data: data files` and `psds` keys, and keep a hook's own bookkeeping (e.g.
"already notified") out of the ledger.

**Optional further reading**, only if you want more than the template's
example gives you:
- A sibling plugin wired up to a *specific* real analysis code, e.g.
  [asimov-pycbc](https://github.com/etive-io/asimov-pycbc) (`pycbc_inference`,
  checkpoint-based resume) or asimov-bayeswave/asimov-lalinference/
  asimov-cogwheel/asimov-pesummary — useful for cross-checking a convention
  or for their end-to-end CI setup (§9). Treat them as evidence of common
  practice, not correct practice: several of them predate the ledger
  vocabulary and use patterns this skill tells you to avoid (e.g.
  asimov-lalinference looks up and chains asimov-pesummary itself, which §6
  rules out).
- Asimov core itself, if you need more than this skill + the template
  cover: `asimov/pipeline.py` (`Pipeline` base class docstrings),
  `asimov/pipelines/testing/simple.py` (asimov's own minimal reference
  pipeline), `asimov/priors.py`, `asimov/analysis.py` (`make_config`),
  `asimov/cli/manage.py` (hook call sites), `asimov/scheduler.py`,
  `asimov/vocabulary.yaml` (every standard ledger key, see §5a).

## 1. Understand the wrapped code first

Before writing a line of pipeline code, answer these by reading the target
code's own source (its CLI entry point and config schema), not by guessing
from its docs:

- **Is it single-job or a real multi-job DAG?** Most modern PE codes
  (`pycbc_inference`, `jim-run`) are a single executable that takes one
  config file and runs to completion — there's no DAG to build.
  `build_dag()` then just checks the run directory and finds the config;
  `submit_dag()` submits one job via `self.scheduler`. Only build an actual
  HTCondor DAG if the code genuinely has multiple dependent stages.
- **Exact config format and schema.** Read the real parsing code (e.g. a
  pydantic model), not just an example config — field names, types, and
  defaults matter for the template/builder you'll write.
- **How does it checkpoint and resume?** Many samplers auto-resume from a
  checkpoint file with no special flag when it's present (e.g. `jim-run`);
  others need an explicit flag suppressed on resume (e.g. `pycbc_inference`
  needs `--force` omitted once a `.checkpoint` file exists). This dictates
  `resurrect()` and part of `submit_dag()`.
  Also check whether the checkpoint path defaults to the same directory as
  final output — if so, give it a separate directory (e.g.
  `<rundir>/checkpoint` vs `<rundir>/output`) so a mid-run checkpoint write
  doesn't collide with a later "don't overwrite existing output" check.
- **What files signal completion, and are any of them written before the
  run has actually finished?** Prefer the *last* file the tool writes
  (check the real write order in source), not the first — e.g. samples are
  usually written before final diagnostics/config-dump, so use the latter
  as the completion marker.
- **Does it need a GPU?** JAX/PyTorch-based samplers usually do — see
  §4 for how to request one through Asimov's scheduler abstraction.

## 2. Rename and start filling in your own logic

`asimov_myplugin/pipeline.py`'s `MyPlugin` class, `_executable()`'s
`"my-executable"` default, and the `myplugin`/`"MyPlugin/..."` names
throughout are placeholders — replace them with your analysis code's real
name. `configs/myplugin.ini` and `priors.py` are similarly meant to be
edited in place: keep the pattern, replace the placeholder fields with your
tool's real config schema and prior types.

## 3. The `Pipeline` hooks that matter, in call order

Asimov calls these at specific, separate CLI steps (`asimov manage build`
vs `asimov manage build submit`) — see `asimov/cli/manage.py` for the exact
call sites, and the docstrings on each method in
`asimov_myplugin/pipeline.py` for a worked example of each. Get the
ordering wrong and the config a later step reads won't have what an
earlier hook was supposed to prepare.

1. **`before_config(dryrun=False)`** — called immediately before
   `production.make_config()` (i.e. before the template in §5 renders).
   Use it to *check* the ledger: raise `PipelineException` if a
   scientifically significant input the template needs (sample rate,
   frequencies, approximant, ...) is missing, rather than letting the
   template fall back on a silent default. **Never write into
   `self.production.meta` here** (or anywhere else in the plugin): the
   ledger holds analysis *inputs* and asimov-managed state, not values
   derived from them. Anything derived — converted priors, a per-detector
   value collapsed to one number, a serialised config — is a method on your
   `Pipeline` which the template calls, because `Analysis.make_config`
   renders the template with `pipeline` in scope (see §5).
2. **`config_template`** (property) — return an absolute path to a bundled
   template file. `Production.make_config()` only uses this when no global
   `[templating] directory` is configured in `asimov.conf`; if one is
   configured, Asimov looks there for a file named `<pipeline-name>.ini`
   **regardless of your template's actual extension** — a pre-existing
   quirk in Asimov core, filed upstream as
   [etive-io/asimov#160](https://github.com/etive-io/asimov/issues/160)
   rather than worked around in the plugin.
3. **`build_dag(dryrun=False)`** — for a single-job tool, read
   `self.production.rundir` and raise `PipelineException` if it is `None`.
   Don't build the path yourself (e.g. from `config.get("general",
   "rundir_default")` plus event/production names): `Analysis.rundir`
   already derives it from the subject's `working directory`, and a second
   derivation in the plugin can disagree with it. Create the directory only
   when not `dryrun`. Then locate the
   already-rendered config in the event repository via
   `self.production.event.repository.find_prods(name, category)`. Raise
   `PipelineException` if it's missing — don't silently call
   `make_config()` from here; that's a separate, earlier CLI step
   (`asimov manage build`), and conflating them breaks the documented
   build → submit separation.
4. **`submit_dag(dryrun=False)`** — call `build_dag()` first, then build a
   submit description dict and submit via `self.scheduler` (§4). Return
   the integer cluster/job ID and set `self.production.status = "running"`
   and `self.production.job_id`.
5. **`detect_completion()`** — check for the completion marker from §1.
6. **`after_completion()`** — call `super().after_completion()` to mark
   `finished`; don't chain into post-processing yourself (see §6).
7. **`resurrect()`** — resubmit on eviction/failure, capped at a small
   retry count. Asimov core has no retry-count field yet, so keep the count
   out of the ledger (the template stores it in a `.resurrections` file in
   the run directory) and raise the gap upstream rather than inventing a
   ledger key.
8. **`collect_assets()` / `collect_logs()` / `samples()`** — return
   whatever a downstream production or Asimov's reporting needs.

## 4. Submitting jobs: use the scheduler abstraction, not raw HTCondor

Don't call `htcondor`/`htcondor2` or `subprocess.run(["condor_submit_dag"])`
directly — use `asimov.scheduler_utils.create_job_from_dict()` and
`self.scheduler.submit(job)` (`self.scheduler` is lazily resolved from
`asimov.conf`, HTCondor or Slurm). This is what makes the plugin work on
both schedulers for free. `asimov_myplugin/pipeline.py`'s `submit_dag` is a
complete working example; the shape is:

```python
submit_description = {
    "executable": executable_path,
    "arguments": " ".join(args),
    "output": ..., "error": ..., "log": ...,
    "request_cpus": str(scheduler_meta.get("request cpus", 1)),
    "request_memory": str(scheduler_meta.get("request memory", "4096MB")),
    "request_disk": str(scheduler_meta.get("request disk", "4096MB")),
    "getenv": "true",
    "batch_name": f"MyPlugin/{event.name}/{production.name}",
}
job = create_job_from_dict(submit_description)
cluster_id = self.scheduler.submit(job)
```

Read resource requests from the standard `scheduler:` keys — `request cpus`,
`request memory`, `request disk`, `request gpus`, `accounting group` — never
invented spellings like `scheduler: cpus` or `scheduler: gpus` (those are
unknown to the vocabulary, so every other pipeline ignores them).

**GPU requests** aren't part of the generic resource mapping
(`cpus`/`memory`/`disk`). Read the count from `scheduler: request gpus` and
pass scheduler-specific keys directly, branching
on scheduler type (`from asimov.scheduler import Slurm`). This is filed as
a feature request upstream
([etive-io/asimov#161](https://github.com/etive-io/asimov/issues/161)); until
it lands, branch explicitly:

```python
if isinstance(self.scheduler, Slurm):
    submit_description["slurm_gres"] = f"gpu:{n_gpus}"   # -> #SBATCH --gres=gpu:N
else:
    submit_description["request_gpus"] = str(n_gpus)      # HTCondor submit key
```

Don't set both unconditionally on the same dict — an HTCondor submit picks
up unrecognised keys as literal ClassAd attributes, so a stray
`slurm_gres` key would leak into the HTCondor submit description.

Wrap `self.scheduler.submit(...)` in `try/except (FileNotFoundError,
RuntimeError)` and re-raise as `PipelineException` with a clear message —
that's what surfaces cleanly in Asimov's own error reporting.

## 5. Config templating: know when to skip the Liquid template

Asimov renders a pipeline's config via `python-liquid`
(`liq.render(production=self, analysis=self, pipeline=pipeline,
config=config)` — actually the `liquidpy` package, which is Jinja2-based
under a Liquid-compatible syntax layer). Two hard-won facts, both handled
in `asimov_myplugin/configs/myplugin.ini`:

- **Chained dict indexing on a missing key raises, it doesn't return
  nil/blank.** `{{ x['a']['b'] }}` errors immediately if `x['a']` doesn't
  exist — unlike genuine Shopify Liquid. Use Jinja2-native `.get(key,
  default)` chains instead, which *do* work in this templating engine and
  are much safer: `{{ production.meta.get('likelihood', {}).get('sample rate') }}`
  (see the template file for more examples). Don't give scientific inputs
  a default in the template; check for them in `before_config` instead.
- **The template can call methods on your pipeline.** `make_config` passes
  `pipeline=pipeline`, and liquidpy evaluates method calls, so
  `{{ pipeline.render_priors() }}` or `{{ pipeline.minimum_frequency() }}`
  work (tested in `tests/test_pipeline.py::TestConfigRendering`). This is
  how derived values reach the config without going through the ledger.
- **For a config format that's type-sensitive (TOML, JSON, YAML — anything
  where `true`/`4.0`/`"str"` are syntactically distinct), don't build it as
  templated text at all.** Jinja2 renders a Python `True` as the string
  `True` (capitalised), which is invalid TOML/JSON. Chasing this through
  `{% if %}...true...{% else %}...false{% endif %}` branches for every
  boolean/string/number field is exactly as much work as building the
  config as a real Python dict and serializing it properly (`tomli_w`,
  `json`, `yaml.safe_dump`, ...), and the latter is type-correct by
  construction and independently unit-testable. Do the latter: build the
  dict and serialize it in a pipeline method, and make the bundled Liquid
  template a one-line call to it:

  ```
  {{ pipeline.render_config() }}
  ```

  Don't compute the text in `before_config()` and stash it in
  `production.meta` for the template to read back: that writes derived
  output into the ledger (and it is stale as soon as the inputs change).

  Free-text formats like this template's own `.ini` don't have this
  problem — building one directly in Liquid (as `configs/myplugin.ini`
  does) is fine and simpler.

## 5a. Ledger keys: use the vocabulary, register only what's yours

Asimov publishes its ledger vocabulary (`asimov/vocabulary.yaml`, the
`asimov vocabulary` CLI; etive-io/asimov#175). **A quantity with the same
meaning to more than one pipeline must use its standard name.** Before
reading any key, ask: *would bilby, RIFT or another pipeline need this
number if it ran the same analysis?* If yes, use (or propose upstream) the
core term; `asimov vocabulary show "likelihood.sample rate"` tells you what
exists. Examples: `interferometers`, `event time`, `waveform: approximant`,
`likelihood: sample rate`, `likelihood: minimum frequency` (a
**per-detector dict** — if your code wants one scalar, collapse it
explicitly, e.g. the lowest value as in `MyPlugin.minimum_frequency()`, and
say so in the docstring and README), `priors`, `psds` (read it through
`production.psds`, which also resolves PSDs from `needs:`), `data: data
files`, and the `scheduler: request ...` keys.

A pipeline namespace (`myplugin:`) may only hold quantities that mean
nothing to any other pipeline — e.g. `myplugin: sample rate` or `myplugin:
prior:` duplicate standard terms and are rejected. Register every term you
do read under your namespace through the `asimov.vocabulary` entry point, so
`asimov vocabulary check` can validate users' blueprints:

```toml
# pyproject.toml -- the entry-point name must be your pipeline name
[project.entry-points."asimov.vocabulary"]
myplugin = "asimov_myplugin:vocabulary"
```

```python
# asimov_myplugin/__init__.py
def vocabulary():
    return importlib.resources.files(__name__).joinpath("vocabulary.yaml")
```

```yaml
# asimov_myplugin/vocabulary.yaml (ship it as package data)
terms:
  myplugin:
    description: Settings which only my-executable understands.
    children:
      warmup steps:
        type: integer
        description: Warm-up iterations my-executable's sampler discards.
```

Run `asimov vocabulary lint` after adding a term: it reports terms which
clash with or duplicate core ones (even another pipeline's term, e.g.
`chains` collides with BayesWave's `likelihood: chains`). Ship an example
blueprint (`examples/`) that exercises your namespaced settings, and check
it with `asimov vocabulary check --pipeline myplugin examples/*.yaml` in CI
(§10). Document the namespace's schema in the README.

## 6. Post-processing: don't chain it yourself

`after_completion()` should just mark the production finished. Don't look
up or submit a downstream post-processing pipeline (e.g. PESummary) from
inside your plugin. Express it as a separate production in the ledger with
a `needs:` dependency on this one — Asimov's own dependency resolution
builds and submits it once this production reaches `finished`, and it
reads this production's output via `collect_assets()["samples"]` /
`production._previous_assets()`. Keeping this decoupled is why this
template's own example (and `asimov-pycbc`/`asimov-jim`) have no knowledge
of what (if anything) consumes their output.

## 7. Priors: convert types, not parameter names or physics conventions

`asimov.priors.PriorInterface` / `PriorSpecification` give a generic,
pipeline-agnostic prior blueprint (`minimum`, `maximum`, `type`, `alpha`,
`mu`, `sigma`, ...). Override `get_prior_interface()` and implement
`convert()` to translate this into the wrapped code's own prior spec
format, for whichever types have an unambiguous field-for-field mapping
(uniform, gaussian, power-law are usually safe) — see
`asimov_myplugin/priors.py` for a complete, tested example covering
uniform and gaussian.

**Do not** try to also translate parameter names or physical conventions
(e.g. Bilby's `chirp_mass`/`mass_ratio`/spin components → another code's
own names and frames) unless you can verify the mapping is exact. Getting
this wrong produces a config that parses fine but samples a different
physical prior than the one written in the ledger — a silent correctness
bug, not a crash. Pass parameter names through unchanged, and document that
the ledger's `priors:` block must use the wrapped code's own parameter
names.

A prior type your converter can't translate exactly must **raise from
`convert()`** (the template turns that into a `PipelineException`). Don't
add an escape-hatch block such as `myplugin: prior: {...}` for types the
converter doesn't cover: it duplicates the standard `priors` block, the two
can silently disagree, and it lets a prior bypass the `PriorInterface`
altogether. Extend the converter instead. Priors are only ever converted
inside the `PriorInterface` subclass returned by `get_prior_interface()`;
don't read `production.meta["priors"]` in the pipeline.

## 8. Tests

`tests/test_pipeline.py` in this template is a complete, working example
to copy (its `mock_production`/`mock_config` fixtures live at the top of
that file directly, rather than a separate `conftest.py` — either
structure works, `pytest` discovers fixtures either way): a `mock_production`
fixture
(`MagicMock` with realistic `.meta`, `.event.repository.find_prods`
returning a config filename, etc.) and a `mock_config` fixture that
monkeypatches `<package>.pipeline.config.get` to a lookup dict — don't hit
real `asimov.conf`. Patch `<package>.pipeline.shutil.which` per-test to
control executable resolution. Test at minimum: init (including the
pipeline-name mismatch check raising `PipelineException`), config assembly
for every branch of your config schema (render the real template, as
`TestConfigRendering` does, and assert it doesn't write to the ledger),
`before_config` raising for each missing required input, `build_dag`
(missing rundir raises, missing-config error, dryrun creates nothing), `submit_dag` (scheduler abstraction used,
GPU branching per scheduler type, dryrun skips submission, missing
executable and scheduler failure both raise `PipelineException` with a
useful message, accounting-group passthrough), `detect_completion` (marker
present/absent, and that a partial/intermediate file alone isn't mistaken
for completion), `samples`/`collect_assets`/`collect_logs`,
`after_completion`, and `resurrect` (resubmits, stops after its retry
cap, and leaves `production.meta` alone). `tests/test_priors.py` is the equivalent pattern for a
`PriorInterface`: test every supported type plus the unsupported-type
error path.

If the templating engine is involved in generating the actual config
(§5), write a small script (not part of the test suite) that renders it
against a realistic mock and round-trips the result through the format's
own parser (e.g. `tomllib.loads`) — do this once while developing, don't
assume the template is correct just because it isn't obviously wrong.

## 9. End-to-end tests: HTCondor (required), Slurm (desirable)

Unit tests (§8) mock the scheduler entirely — they don't catch a submit
description HTCondor or Slurm actually rejects, a real checkpoint/resume
cycle, or a completion marker that never appears against real output. Add
an end-to-end CI job that installs a real scheduler and your analysis
code, submits a real (small/fast) production, and asserts it reaches
`finished` with the expected output files:

- **HTCondor** is the baseline to cover — most Asimov deployments run on
  it. `asimov-pycbc`'s and `asimov-bayeswave`'s
  `.github/workflows/e2e.yml` (and their
  `.github/actions/setup-*-env/action.yml`) are complete, working
  references: install HTCondor + your tool in the CI runner, run
  `asimov manage build submit`, poll `asimov monitor`/the production
  status for `finished`, and assert the expected output files exist.
  They also cover a real gotcha worth porting: an editable pip install's
  entry-point metadata can end up unreadable by the user HTCondor jobs run
  as under a restrictive root umask, silently dropping your plugin from
  Asimov's `entry_points(group="asimov.pipelines")` discovery — force
  world-readable permissions on the installed environment as a CI step
  (see those `setup-*-env` actions).
- **Slurm** is desirable in addition, not instead — if your deployment
  target includes Slurm clusters, add an equivalent job using a
  containerised single-node Slurm setup (e.g. `giovtorres/slurm-docker-cluster`
  or similar) so the GPU/scheduler branching from §4 actually gets
  exercised on both code paths, not just unit-tested with a mock.

This template's own CI (`.github/workflows/tests.yml`) only runs the unit
tests — it doesn't wrap a real analysis code, so there's nothing for an
e2e job here to submit. Add the e2e workflow once you've filled in your
plugin's real executable and config.

## 10. Packaging, CI, README

Mirror this template's own `pyproject.toml`: `setuptools`,
`[project.entry-points."asimov.pipelines"]` mapping your short pipeline
name to the class, `[project.entry-points."asimov.vocabulary"]` for your
namespaced terms (§5a), `[tool.setuptools.package-data]` for any bundled
config templates and `vocabulary.yaml`, `[project.optional-dependencies]` with a `dev`/`test` extra
(`pytest`, `pytest-cov`). Depend on `asimov>=0.7` — **not** the wrapped
analysis code itself (that's expected to already be in the runtime
environment jobs execute in, same as `asimov-pycbc` doesn't depend on
`pycbc`).

CI: this template's own `.github/workflows/tests.yml` (plain `pip install
-e ".[dev]"` + `pytest --cov` + `black --check` + `flake8`, plus a
`vocabulary` job running `asimov vocabulary check --pipeline myplugin
examples/*.yaml` and `asimov vocabulary lint`) is a complete starting
point. Add the e2e workflow from §9 once your plugin wraps a real
executable.

Before opening a PR, run the strict reviewer's linter from
[claude-gw](https://github.com/transientlunatic/claude-gw)
(`claude/.claude/skills/asimov-plugin-review/scripts/lint_plugin.py <plugin>
--asimov-src <asimov checkout>`) and fix every BLOCKER and MUST-FIX it
reports. This template itself passes it with none.

README: document the ledger `pipeline:` name, the standard ledger keys the
plugin reads (and how per-detector values are collapsed), any
pipeline-specific metadata block and its schema, GPU/scheduler requests if relevant, the
checkpoint/resume story, and the post-processing `needs:` pattern from §6.

## 11. Opening the PR

If the target repository is brand new and empty, GitHub still needs its
default branch (usually `main`) to exist as a real ref before a PR can be
opened against it — push a minimal bootstrap commit (LICENSE + .gitignore
is enough) to `main` first if nothing else has, then branch off it for the
actual plugin work, so the PR shows a meaningful diff rather than the
entire repository as unreviewable.
