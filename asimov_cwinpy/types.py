"""
The cwinpy analysis type: ``type: cwinpy``.

The analyses of the cwinpy pipelines are generic ones: a pulsar has no
gravitational-wave transient metadata (no likelihood, waveform or event time) and
asimov does not look for PSDs for it. The type also brings to the ledger
vocabulary the terms that more than one of the pipelines reads, which are
therefore not owned by any one pipeline.
"""

from asimov.analysis import SimpleAnalysis


class CwinpyAnalysis(SimpleAnalysis):
    """An analysis by one of the cwinpy pipelines."""

    #: The terms shared by the cwinpy pipelines, in the format of a plugin's
    #: vocabulary. Settings that only one pipeline understands are in the
    #: vocabulary registered under that pipeline's name.
    vocabulary = {
        "terms": {
            "ephemeris": {
                "type": "string",
                "description": (
                    "The path to the Tempo(2)-style pulsar parameter (.par) file "
                    "of the pulsar, which gives its position and its rotation "
                    "frequency and phase evolution."
                ),
            },
            "frequency factors": {
                "type": "list",
                "description": (
                    "The factors of the rotation frequency of the pulsar at which "
                    "its data is heterodyned and analysed, e.g. [1, 2] for the "
                    "signal at once and twice the rotation frequency."
                ),
            },
        }
    }
