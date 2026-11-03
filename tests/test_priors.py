"""The conversion of asimov's priors into the prior file of cwinpy_pe."""

import pytest

from asimov_cwinpy.priors import CwinpyPriorInterface


def converted(priors):
    return CwinpyPriorInterface(priors).convert()


def test_there_is_nothing_to_convert_without_priors():
    assert converted(None) == {}
    assert CwinpyPriorInterface(None).render() == ""


def test_a_uniform_prior():
    result = converted({"h0": {"type": "uniform", "minimum": 0.0, "maximum": 1e-22}})
    assert result == {"h0": "Uniform(minimum=0.0, maximum=1e-22, name='h0')"}


def test_a_range_with_no_type_is_uniform():
    assert converted({"psi": {"minimum": 0.0, "maximum": 1.5}})["psi"].startswith("Uniform(")


@pytest.mark.parametrize(
    "kind, bilby",
    [("sine", "Sine"), ("cosine", "Cosine"), ("loguniform", "LogUniform"), ("UNIFORM", "Uniform")],
)
def test_the_types_with_a_range(kind, bilby):
    result = converted({"x": {"type": kind, "minimum": 1.0, "maximum": 2.0}})
    assert result["x"] == f"{bilby}(minimum=1.0, maximum=2.0, name='x')"


@pytest.mark.parametrize("kind", ["gaussian", "normal"])
def test_a_gaussian_prior(kind):
    result = converted({"x": {"type": kind, "mu": 1.0, "sigma": 0.5}})
    assert result["x"] == "Gaussian(mu=1.0, sigma=0.5, name='x')"


def test_a_power_law_prior():
    result = converted({"x": {"type": "powerlaw", "alpha": -2.0, "minimum": 1.0, "maximum": 9.0}})
    assert result["x"] == "PowerLaw(alpha=-2.0, minimum=1.0, maximum=9.0, name='x')"


def test_a_boundary_is_passed_on_and_none_is_not():
    spec = {"type": "uniform", "minimum": 0.0, "maximum": 1.0}
    assert "boundary='periodic'" in converted({"x": {**spec, "boundary": "periodic"}})["x"]
    assert "boundary" not in converted({"x": {**spec, "boundary": "None"}})["x"]


def test_the_names_of_the_parameters_are_not_changed():
    names = ["h0", "cosiota", "c21", "phi22"]
    spec = {"type": "uniform", "minimum": 0.0, "maximum": 1.0}
    assert list(converted({name: spec for name in names})) == names


def test_a_type_that_cannot_be_translated_is_an_error():
    with pytest.raises(ValueError, match="not one that cwinpy-pe can use"):
        converted({"x": {"type": "cauchy", "minimum": 0.0, "maximum": 1.0}})


def test_a_prior_with_neither_a_type_nor_a_range_is_an_error():
    with pytest.raises(ValueError, match="not one that cwinpy-pe can use"):
        converted({"x": {"minimum": 0.0}})


def test_a_prior_that_lacks_a_value_is_an_error():
    with pytest.raises(ValueError, match="needs a value for 'sigma'"):
        converted({"x": {"type": "gaussian", "mu": 1.0}})


def test_the_prior_file_has_a_line_for_each_prior():
    text = CwinpyPriorInterface(
        {
            "h0": {"type": "uniform", "minimum": 0.0, "maximum": 1e-22},
            "iota": {"type": "sine", "minimum": 0.0, "maximum": 3.0},
        }
    ).render()
    assert text.splitlines() == [
        "h0 = Uniform(minimum=0.0, maximum=1e-22, name='h0')",
        "iota = Sine(minimum=0.0, maximum=3.0, name='iota')",
    ]
    assert text.endswith("\n")
