"""The plugin in a real asimov project: the examples are applied and built as designed."""

import os
import shutil
import tempfile
import unittest

from asimov.analysis import GravitationalWaveTransient
from asimov.analysis_types import reset_analysis_types
from asimov.cli.application import apply_page
from asimov.cli.project import make_project
from asimov.ledger import YAMLLedger

from asimov_cwinpy.heterodyne import HeterodynePipeline
from asimov_cwinpy.merge import MergePipeline
from asimov_cwinpy.pe import PEPipeline
from asimov_cwinpy.types import CwinpyAnalysis

EXAMPLES = os.path.join(os.path.dirname(__file__), "..", "examples")


class ExamplesTests(unittest.TestCase):
    def setUp(self):
        reset_analysis_types()
        self.addCleanup(reset_analysis_types)
        self.cwd = os.getcwd()
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.cwd)
        make_project(name="cwinpy test", root=self.root, engine="yamlfile")
        self.ledger = YAMLLedger(".asimov/ledger.yml")
        # The run subject first, as the pulsar's analysis needs one of its analyses.
        apply_page(os.path.join(EXAMPLES, "run.yaml"), ledger=self.ledger)
        apply_page(os.path.join(EXAMPLES, "pulsar.yaml"), ledger=self.ledger)
        self.loaded = YAMLLedger(".asimov/ledger.yml")

    def analyses(self, subject):
        return {a.name: a for a in self.loaded.get_event(subject)[0].productions}

    def test_the_analyses_are_cwinpy_analyses_and_not_gravitational_wave_ones(self):
        for subject in ("O4a-heterodyne", "J0534+2200"):
            for analysis in self.analyses(subject).values():
                self.assertIs(type(analysis), CwinpyAnalysis, analysis.name)
                self.assertNotIsInstance(analysis, GravitationalWaveTransient)

    def test_a_pulsar_analysis_has_no_gravitational_wave_metadata(self):
        meta = self.analyses("J0534+2200")["pe"].meta
        for key in ("waveform", "event time", "data", "quality"):
            self.assertNotIn(key, meta)
        # (asimov's generic analysis still starts with empty `likelihood` and `sampler`
        # blocks; what a gravitational-wave analysis adds to them is not there.)
        self.assertEqual(meta.get("likelihood"), {})
        self.assertEqual(meta.get("sampler"), {})

    def test_each_analysis_has_the_pipeline_which_it_names(self):
        shard = self.analyses("O4a-heterodyne")["shard-0001"]
        self.assertIsInstance(shard.pipeline, HeterodynePipeline)
        self.assertIsInstance(self.analyses("O4a-heterodyne")["merge"].pipeline, MergePipeline)
        self.assertIsInstance(self.analyses("J0534+2200")["pe"].pipeline, PEPipeline)

    def test_the_merge_needs_the_shard(self):
        self.assertEqual(self.analyses("O4a-heterodyne")["merge"].dependencies, ["shard-0001"])

    def test_the_estimation_of_a_pulsar_needs_the_merge_of_the_run(self):
        pe = self.analyses("J0534+2200")["pe"]
        self.assertEqual(pe.dependencies, ["O4a-heterodyne/merge"])

    def test_the_settings_of_the_pulsar_and_of_the_shard_are_in_the_metadata(self):
        self.assertEqual(
            self.analyses("J0534+2200")["pe"].meta["ephemeris"], "/path/to/J0534+2200.par"
        )
        shard = self.analyses("O4a-heterodyne")["shard-0001"]
        self.assertEqual(shard.meta["cwinpy-heterodyne"]["joblength"], 86400)
        self.assertEqual(shard.meta["frequency factors"], [1, 2])
