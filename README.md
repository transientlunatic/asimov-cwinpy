# Asimov Plugin Template

A comprehensive template for creating [asimov](https://github.com/transientlunatic/asimov) plugins with support for custom pipelines, applicator hooks, and post-monitor hooks.

> **Using Claude Code?** This repo ships a
> [`asimov-plugin` skill](.claude/skills/asimov-plugin/SKILL.md) distilled
> from building a real plugin
> ([asimov-jim](https://github.com/transientlunatic/asimov-jim)). It walks
> through the example package in this template (single-job submission via
> Asimov's scheduler abstraction, GPU requests, the Liquid-templating
> gotcha for type-sensitive config formats like TOML/JSON, and the
> type-only prior-conversion pattern) plus how to add end-to-end scheduler
> tests, so a session starting a new plugin doesn't have to rediscover
> these from scratch.

## Features

This template provides:

- **Pipeline Plugin**: Integrate custom parameter estimation or analysis pipelines with asimov's workflow management
- **Applicator Hook**: Provide data and configuration to asimov projects
- **Post-Monitor Hook**: Run custom code after each monitoring cycle
- Fully documented example implementations
- Test infrastructure with pytest
- CI/CD workflow with GitHub Actions
- Modern Python packaging with pyproject.toml

## Quick Start

### 1. Clone and Rename

```bash
# Clone this template
git clone https://github.com/yourusername/asimov-plugin-template.git asimov-myplugin
cd asimov-myplugin

# Rename the package
mv asimov_myplugin asimov_yourplugin

# Update pyproject.toml with your plugin name and details
```

### 2. Customize Your Plugin

Edit these key files:

- `pyproject.toml`: Update package name, dependencies, and entry points
- `asimov_yourplugin/pipeline.py`: Implement your pipeline logic
- `asimov_yourplugin/hooks.py`: Customize applicator and monitor hooks
- `asimov_yourplugin/__init__.py`: Update imports

### 3. Install for Development

```bash
pip install -e ".[dev]"
```

### 4. Test Your Plugin

```bash
# Run tests
pytest

# Check your plugin is registered
python -c "from importlib.metadata import entry_points; print([ep.name for ep in entry_points(group='asimov.pipelines')])"
```

## Plugin Types

### Pipeline Plugin

Pipelines integrate custom analysis codes with asimov's workflow management system.

**Use cases:**
- Parameter estimation pipelines (Bilby, LALInference, etc.)
- Data processing workflows
- Custom analysis tools

**Key methods to implement:**
- `build_dag()`: Resolve the run directory and locate the rendered config file (most modern PE codes are a single executable taking one config file, so there's no real DAG to build — see [HTCondor/Slurm Integration](#htcondorslurm-integration) below)
- `submit_dag()`: Submit the job to the scheduler and return the cluster/job ID
- `detect_completion()`: Check if the job completed
- `after_completion()`: Mark the production finished (post-processing is a separate production with a `needs:` dependency — see [Configuration](#configuration))

**Example usage in ledger:**
```yaml
productions:
  - name: my_analysis
    pipeline: myplugin  # Your pipeline name from entry_points
    status: ready
```

**Run with:**
```bash
asimov manage build my_analysis   # Build DAG
asimov manage submit my_analysis  # Submit job
asimov monitor                    # Monitor execution
```

### Applicator Hook

Applicator hooks provide data and configuration to asimov projects.

**Use cases:**
- Fetch strain data from GWOSC or data grid
- Generate configuration files from templates
- Set up analysis parameters
- Download reference files (PSDs, calibration)

**Key methods to implement:**
- `__init__(ledger)`: Initialize with asimov ledger
- `run(event)`: Execute hook for specific event

**Example usage:**
```bash
asimov apply -p my-applicator -e GW150914
```

### Post-Monitor Hook

Post-monitor hooks run after each monitoring cycle.

**Use cases:**
- Send notifications about job status changes
- Update external dashboards or databases
- Generate progress reports
- Trigger downstream workflows
- Archive completed results

**Key methods to implement:**
- `__init__(ledger)`: Initialize with asimov ledger
- `run()`: Execute hook after monitoring cycle

**Configuration in ledger:**
```yaml
hooks:
  postmonitor:
    - my-monitor  # Your hook name from entry_points
```

**Runs automatically with:**
```bash
asimov monitor
```

## File Structure

```
asimov-myplugin/
├── asimov_myplugin/          # Main package
│   ├── __init__.py           # Package initialization
│   ├── pipeline.py           # Pipeline implementation
│   ├── hooks.py              # Applicator and monitor hooks
│   ├── priors.py             # Prior interface (converts priors: types)
│   ├── vocabulary.yaml       # Ledger terms registered under `myplugin:`
│   └── configs/
│       └── myplugin.ini      # Bundled Liquid config template
├── examples/
│   └── myplugin-analysis.yaml  # Example blueprint (checked in CI)
├── tests/                    # Test suite
│   ├── __init__.py
│   ├── test_pipeline.py
│   ├── test_priors.py
│   └── test_hooks.py
├── .github/
│   └── workflows/
│       └── tests.yml         # CI/CD workflow
├── pyproject.toml            # Package configuration
├── README.md                 # This file
├── LICENSE                   # License file
└── .gitignore                # Git ignore rules
```

## Development Guide

### Pipeline Development

1. **Inherit from Pipeline base class:**
   ```python
   from asimov.pipeline import Pipeline

   class MyPlugin(Pipeline):
       name = "My Plugin"

       def __init__(self, production, category=None):
           super().__init__(production, category)
   ```

2. **Implement required methods:**
   - `build_dag()`: Resolve the run directory and locate the rendered config
     (see [HTCondor/Slurm Integration](#htcondorslurm-integration))
   - `submit_dag()`: Submit to the scheduler via `self.scheduler` and return the cluster/job ID

3. **Implement optional lifecycle hooks:**
   - `before_config()`: Check the ledger supplies the template's required inputs
     (raise `PipelineException` if not); never write into `production.meta`
   - `before_build()`: Pre-build validation
   - `before_submit()`: Pre-submission modifications
   - `after_completion()`: Post-processing
   - `after_processing()`: Final cleanup

4. **Access production metadata:**
   ```python
   self.production.name          # Production name
   self.production.rundir        # Run directory
   self.production.event         # Parent event object
   self.production.meta          # Configuration dict
   self.production.status        # Current status
   ```

5. **Don't write to production metadata.** The ledger holds analysis
   *inputs*. Return results from `collect_assets()` under the standard asset
   names (`samples`, `psds`, ...), and compute derived values in a pipeline
   method the config template calls (`{{ pipeline.render_priors() }}`)
   rather than storing them in `production.meta`.

### Hook Development

1. **Implement hook class:**
   ```python
   class MyApplicatorHook:
       def __init__(self, ledger):
           self.ledger = ledger

       def run(self, event):
           # Your implementation
           pass
   ```

2. **Access ledger data:**
   ```python
   # Get event
   event_obj = self.ledger.events[event_name]

   # Iterate events
   for event in self.ledger.events.values():
       for production in event.productions:
           # Process production
           pass
   ```

3. **Record inputs under their standard names only:**
   ```python
   # Fetched data, keyed by detector (see MyApplicatorHook._record_data_files)
   event_obj.meta.setdefault("data", {})["data files"] = {"H1": "/path/H1.gwf"}
   event_obj.meta["psds"] = {"H1": "/path/H1-psd.dat"}
   ```
   Don't invent keys (`data_files`, `custom_field`, `notified`...) or store a
   hook's own bookkeeping in the ledger; `asimov vocabulary show <key>`
   lists the standard ones.

### Testing

Create tests in the `tests/` directory:

```python
# tests/test_pipeline.py
import pytest
from asimov_myplugin.pipeline import MyPlugin

def test_pipeline_initialization():
    # Mock production object
    from unittest.mock import Mock
    production = Mock()
    production.name = "test"
    production.rundir = "/tmp/test"

    # Test initialization
    pipeline = MyPlugin(production)
    assert pipeline.name == "My Plugin"
    assert pipeline.production == production
```

Run tests:
```bash
pytest                          # Run all tests
pytest -v                       # Verbose output
pytest --cov=asimov_myplugin   # With coverage
```

### Entry Points

Configure entry points in `pyproject.toml`:

```toml
[project.entry-points."asimov.pipelines"]
myplugin = "asimov_myplugin.pipeline:MyPlugin"

[project.entry-points."asimov.vocabulary"]
myplugin = "asimov_myplugin:vocabulary"

[project.entry-points."asimov.hooks.applicator"]
my-applicator = "asimov_myplugin.hooks:MyApplicatorHook"

[project.entry-points."asimov.hooks.postmonitor"]
my-monitor = "asimov_myplugin.hooks:MyPostMonitorHook"
```

The entry point name (left side) is what users will reference in their ledger files or command line.

## Configuration

### Pipeline Configuration

Users configure your pipeline in their ledger YAML:

```yaml
kind: analysis
name: my_analysis
pipeline: myplugin

# Standard ledger keys, shared with every other pipeline
waveform:
  approximant: IMRPhenomXPHM
likelihood:
  sample rate: 4096
  minimum frequency:   # per detector; myplugin uses the lowest
    H1: 20
    L1: 20
scheduler:
  request cpus: 4
  request gpus: 1

# Settings only my-executable understands, registered in
# asimov_myplugin/vocabulary.yaml
myplugin:
  warmup steps: 1000
```

`examples/myplugin-analysis.yaml` is a complete example, checked in CI with
`asimov vocabulary check --pipeline myplugin examples/*.yaml`.

The plugin reads these standard keys: `event time`, `interferometers`,
`waveform: approximant`, `likelihood: sample rate`, `likelihood: minimum
frequency` (the lowest per-detector value is passed to `my-executable`),
`priors` and the `scheduler: request ...` / `accounting group` keys. The
first five are required: `asimov manage build` fails with a
`PipelineException` if one is missing.

Its own namespace, `myplugin:`, holds only:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `warmup steps` | integer | 500 | Warm-up iterations my-executable's sampler discards |

Register every key you add under your namespace in `vocabulary.yaml` (the
`asimov.vocabulary` entry point), and never put a quantity other pipelines
also need (sample rate, frequencies, PSDs, priors) under it.

### Global Pipeline Defaults

Users can set project-wide defaults for any of the keys above in their
ledger, using the same vocabulary:

```yaml
pipelines:
  myplugin:
    likelihood:
      sample rate: 4096
    myplugin:
      warmup steps: 1000
```

### Hook Configuration

Post-monitor hooks are enabled in the ledger:

```yaml
hooks:
  postmonitor:
    - my-monitor
    - another-hook
```

## HTCondor/Slurm Integration

Most modern PE codes (`pycbc_inference`, `jim-run`, ...) are a single
executable that takes one config file and runs to completion — there's no
DAG to build. Use Asimov's scheduler abstraction
(`asimov.scheduler_utils.create_job_from_dict` + `self.scheduler.submit()`)
rather than hand-rolling `htcondor`/`htcondor2` calls or a
`condor_submit_dag` subprocess — this is what makes the plugin work
unmodified on both HTCondor and Slurm. See `asimov_myplugin/pipeline.py`
for the full, working example; the shape is:

```python
def build_dag(self, dryrun=False):
    # Read self.production.rundir (raise PipelineException if it is None;
    # asimov derives it from the subject's working directory) and locate
    # the rendered config file
    # in the event repository (see asimov_myplugin/pipeline.py). Return
    # its path -- there's nothing to actually build for a single-job tool.
    ...

def submit_dag(self, dryrun=False):
    config_file = self.build_dag(dryrun=dryrun)
    submit_description = {
        "executable": self._executable(),
        "arguments": config_file,
        "output": ..., "error": ..., "log": ...,
        "request_cpus": str(scheduler_meta.get("request cpus", 1)),
        "request_memory": str(scheduler_meta.get("request memory", "4096MB")),
        "getenv": "true",
    }
    # GPU requests aren't part of the generic cpus/memory/disk mapping
    # (https://github.com/etive-io/asimov/issues/161) -- branch on
    # scheduler type if your tool needs one:
    if isinstance(self.scheduler, Slurm):
        submit_description["slurm_gres"] = "gpu:1"
    else:
        submit_description["request_gpus"] = "1"

    job = create_job_from_dict(submit_description)
    cluster_id = self.scheduler.submit(job)
    self.production.status = "running"
    self.production.job_id = int(cluster_id)
    return int(cluster_id)
```

If your tool genuinely has multiple dependent stages, build a real
HTCondor DAG file and call `self.scheduler.submit_dag()` with it instead
of `self.scheduler.submit()` with a single job.

## Prior Interfaces

If your pipeline needs custom priors, implement a `PriorInterface`
(`asimov_myplugin/priors.py` has a complete, tested example):

```python
# asimov_myplugin/priors.py
from asimov.priors import PriorInterface

class MyPluginPriorInterface(PriorInterface):
    def convert(self):
        """Convert self.prior_dict (Asimov's generic prior blueprint) into
        this tool's own prior spec format."""
        if self.prior_dict is None:
            return {}
        converted = {}
        for name in self.prior_dict.to_dict():
            if name == "default":
                continue  # not a parameter -- a Bilby-style default prior set name
            spec = self.prior_dict.get_prior(name)
            if spec is None:
                continue
            # ... translate spec.type/minimum/maximum/... into your format
        return converted

# In your pipeline class:
def get_prior_interface(self):
    from .priors import MyPluginPriorInterface
    if self._prior_interface is None:
        self._prior_interface = MyPluginPriorInterface(self.production.priors)
    return self._prior_interface
```

**Convert prior *types* (uniform, gaussian, ...), never parameter names or
physical conventions** (e.g. Bilby's `chirp_mass`/`mass_ratio`/spin
components → another code's own names and frames), unless you can verify
the mapping is exact. Getting a type wrong is easy to spot — the config
fails to validate. Getting a parameter-name or convention mapping wrong
produces a config that parses fine but samples a *different* physical
prior than the one written in the ledger: a silent correctness bug, not a
crash. Pass parameter names through unchanged, and document that the
ledger's `priors:` block must use your tool's own parameter names.

## Publishing Your Plugin

1. **Update package metadata:**
   - Name, version, description in `pyproject.toml`
   - Add your name and contact info
   - Choose appropriate license

2. **Write documentation:**
   - Installation instructions
   - Configuration examples
   - Usage guide

3. **Set up CI/CD:**
   - Configure GitHub Actions (provided in template)
   - Add tests for your functionality

4. **Publish to PyPI:**
   ```bash
   # Build package
   python -m build

   # Upload to PyPI
   python -m twine upload dist/*
   ```

5. **Let users install:**
   ```bash
   pip install asimov-yourplugin
   ```

## Examples

### Example Pipeline Usage

```yaml
# ledger.yml
productions:
  - name: GW150914_analysis
    pipeline: myplugin
    status: ready

    sampler:
      nlive: 2000

    quality:
      minimum frequency:
        H1: 20
        L1: 20
```

```bash
# Build and submit
asimov manage build GW150914_analysis
asimov manage submit GW150914_analysis

# Monitor
asimov monitor
```

### Example Applicator Usage

```bash
# Fetch data and set up analysis
asimov apply -p my-applicator -e GW150914
```

### Example Monitor Hook

Automatically runs during monitoring - no separate command needed.

## Troubleshooting

### Plugin not discovered

```bash
# Check if entry points are registered
python -c "from importlib.metadata import entry_points; print(list(entry_points(group='asimov.pipelines')))"

# Reinstall in editable mode
pip uninstall asimov-myplugin
pip install -e .
```

### Import errors

Check that:
1. Package name matches in `pyproject.toml` and imports
2. All `__init__.py` files exist
3. Dependencies are installed

### Job submission fails

Check that:
1. Your scheduler is configured in `asimov.conf` (`[scheduler] type = htcondor` or `slurm`)
2. The analysis executable resolves: `shutil.which("my-executable")` from within the environment `asimov` runs jobs in
3. Submit description paths (`output`/`error`/`log`, run directory) exist and are writable

## Resources

- [Asimov Documentation](https://github.com/transientlunatic/asimov)
- [HTCondor Documentation](https://htcondor.readthedocs.io/)
- [Python Packaging Guide](https://packaging.python.org/)

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

For issues with this template:
- Open an issue on GitHub
- Contact the maintainers

For asimov-related questions:
- See [asimov documentation](https://github.com/transientlunatic/asimov)
- Open an issue on the asimov repository
