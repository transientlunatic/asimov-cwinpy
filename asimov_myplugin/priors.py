"""Prior interface between Asimov's generic prior blueprint and this plugin's format.

Convert prior *types* (uniform, gaussian, ...), never parameter names or
physical conventions (e.g. Bilby's ``chirp_mass``/``mass_ratio`` -> another
code's own names and spin/distance frames). Getting a type conversion wrong
is easy to spot -- the config fails to validate. Getting a parameter-name
or convention mapping wrong produces a config that parses fine but samples
a *different* physical prior than the one written in the ledger: a silent
correctness bug, not a crash. Pass parameter names through unchanged, and
document that the ledger's ``priors:`` block must use your tool's own
parameter names.

A prior type this interface can't translate exactly is an error, raised
from ``convert()``. Don't add a plugin-specific ``myplugin: prior:`` block
as an escape hatch: that duplicates the standard ``priors`` block, and the
two can silently disagree.
"""

from asimov.priors import PriorInterface

# Map Asimov's generic PriorSpecification.type values to this tool's own
# type names. Extend this for whatever your tool actually supports -- only
# list types where the field-for-field mapping below is unambiguous.
_TYPE_MAP = {
    "uniform": "uniform",
    "gaussian": "gaussian",
    "normal": "gaussian",
}


class MyPluginPriorInterface(PriorInterface):
    """Convert an Asimov prior blueprint into this plugin's own prior spec dict."""

    def convert(self):
        if self.prior_dict is None:
            return {}

        converted = {}
        for name in self.prior_dict.to_dict():
            if name == "default":
                continue
            spec = self.prior_dict.get_prior(name)
            if spec is None:
                continue
            converted[name] = self._convert_one(name, spec)
        return converted

    def _convert_one(self, name, spec):
        prior_type = (spec.type or "").lower()
        if prior_type not in _TYPE_MAP:
            raise ValueError(
                f"Cannot convert prior for '{name}': this interface does not "
                f"know how to translate type '{spec.type}'. Supported types: "
                f"{sorted(set(_TYPE_MAP.keys()))}. Extend _TYPE_MAP (and "
                "_convert_one) if my-executable supports an exact equivalent."
            )
        my_type = _TYPE_MAP[prior_type]
        if my_type == "uniform":
            return {"type": "uniform", "min": spec.minimum, "max": spec.maximum}
        if my_type == "gaussian":
            return {"type": "gaussian", "mean": spec.mu, "std": spec.sigma}
        raise AssertionError(f"unreachable: unhandled type {my_type!r}")  # pragma: no cover
