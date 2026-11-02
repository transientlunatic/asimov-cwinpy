"""Tests for the example prior interface."""

import pytest

from asimov_myplugin.priors import MyPluginPriorInterface


class TestMyPluginPriorInterface:
    def test_convert_none_returns_empty_dict(self):
        interface = MyPluginPriorInterface(None)
        assert interface.convert() == {}

    def test_convert_uniform(self):
        interface = MyPluginPriorInterface(
            {"mass": {"type": "uniform", "minimum": 10.0, "maximum": 80.0}}
        )
        assert interface.convert() == {"mass": {"type": "uniform", "min": 10.0, "max": 80.0}}

    def test_convert_gaussian(self):
        interface = MyPluginPriorInterface({"psi": {"type": "gaussian", "mu": 0.0, "sigma": 1.0}})
        assert interface.convert() == {"psi": {"type": "gaussian", "mean": 0.0, "std": 1.0}}

    def test_convert_skips_default_key(self):
        interface = MyPluginPriorInterface(
            {"default": "BBHPriorDict", "mass": {"type": "uniform", "minimum": 1.0, "maximum": 2.0}}
        )
        assert set(interface.convert().keys()) == {"mass"}

    def test_convert_unknown_type_raises_clear_error(self):
        interface = MyPluginPriorInterface({"a_1": {"type": "power_law"}})
        with pytest.raises(ValueError, match="does not know how to translate"):
            interface.convert()
