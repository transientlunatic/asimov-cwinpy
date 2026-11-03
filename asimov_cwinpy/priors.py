"""
Prior interface between asimov's generic ``priors`` block and ``cwinpy_pe``.

``cwinpy_pe`` takes its prior as a bilby-style prior file, with one line per
parameter, such as::

    h0 = Uniform(minimum=0.0, maximum=1e-21, name='h0')

This converts the *types* of priors (uniform, Gaussian, ...). It does not
translate parameter names: they are passed through unchanged, so the names in
the ledger's ``priors`` block must be the ones ``cwinpy_pe`` uses (``h0``,
``phi0``, ``iota``, ``psi``, ``cosiota``, ``c21``, ``c22``, ...). A prior of a
type that cannot be translated exactly is an error, raised from
:meth:`CwinpyPriorInterface.convert`, rather than a prior that quietly differs
from the one written in the ledger.
"""

from asimov.priors import PriorInterface

#: asimov's names for the types of prior, and the bilby class for each, with the
#: attributes of the prior specification that the class takes.
_TYPES = {
    "uniform": ("Uniform", ("minimum", "maximum")),
    "loguniform": ("LogUniform", ("minimum", "maximum")),
    "sine": ("Sine", ("minimum", "maximum")),
    "cosine": ("Cosine", ("minimum", "maximum")),
    "powerlaw": ("PowerLaw", ("alpha", "minimum", "maximum")),
    "gaussian": ("Gaussian", ("mu", "sigma")),
    "normal": ("Gaussian", ("mu", "sigma")),
}

#: The names bilby gives these arguments.
_KEYWORDS = {
    "minimum": "minimum",
    "maximum": "maximum",
    "alpha": "alpha",
    "mu": "mu",
    "sigma": "sigma",
}


class CwinpyPriorInterface(PriorInterface):
    """Convert asimov's priors into the text of a bilby prior file for ``cwinpy_pe``."""

    def convert(self):
        """
        Convert the priors.

        Returns
        -------
        dict
            ``{parameter: line}``, where ``line`` is the right hand side of the
            line of the prior file, e.g. ``"Uniform(minimum=0.0, maximum=1.0, name='h0')"``.
            Empty if there are no priors.

        Raises
        ------
        ValueError
            If a prior has a type that cannot be translated, or lacks a value
            that its type needs.
        """
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

    def render(self):
        """
        The text of the prior file.

        Returns
        -------
        str
            One ``parameter = Prior(...)`` line for each prior, ending in a newline.
        """
        return "".join(f"{name} = {line}\n" for name, line in self.convert().items())

    @staticmethod
    def _convert_one(name, spec):
        prior_type = (spec.type or "").lower()
        if not prior_type and spec.minimum is not None and spec.maximum is not None:
            # A range and nothing else is a uniform prior.
            prior_type = "uniform"
        if prior_type not in _TYPES:
            raise ValueError(
                f"Cannot convert the prior of '{name}': the type '{spec.type}' is not one "
                f"that cwinpy-pe can use. The types are {sorted(_TYPES)}."
            )
        bilby_class, needed = _TYPES[prior_type]
        arguments = []
        for attribute in needed:
            value = getattr(spec, attribute)
            if value is None:
                raise ValueError(
                    f"The {prior_type} prior of '{name}' needs a value for '{attribute}'."
                )
            arguments.append(f"{_KEYWORDS[attribute]}={float(value)!r}")
        if spec.boundary and spec.boundary != "None":
            arguments.append(f"boundary={spec.boundary!r}")
        arguments.append(f"name={name!r}")
        return f"{bilby_class}({', '.join(arguments)})"
