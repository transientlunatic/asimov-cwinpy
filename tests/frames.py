"""Small fake frames, so that the real cwinpy_heterodyne can be run."""

import numpy as np

#: A pulsar at 29.6 Hz, which a 256 Hz sample rate is plenty for.
PAR = """\
PSRJ     J0534+2200
RAJ      05:34:31.97232
DECJ     22:00:52.0690
F0       29.6
PEPOCH   55555
"""

START = 1000000000
FRAME = 1024
RATE = 256


def make_frames(root, detectors=("H1", "L1"), count=2):
    """
    Write ``count`` frame files of noise for each detector, in ``root/<detector>``.

    Returns
    -------
    dict
        ``{"end": GPS end time, "caches": {detector: directory}}``.
    """
    from gwpy.timeseries import TimeSeries

    rng = np.random.default_rng(1)
    caches = {}
    for detector in detectors:
        directory = root / detector
        directory.mkdir(parents=True, exist_ok=True)
        for k in range(count):
            start = START + k * FRAME
            series = TimeSeries(
                rng.normal(0, 1e-23, FRAME * RATE),
                t0=start,
                sample_rate=RATE,
                channel=f"{detector}:FAKE-STRAIN",
            )
            series.write(
                str(directory / f"{detector[0]}-{detector}_FAKE-{start}-{FRAME}.gwf"),
                format="gwf.lalframe",
            )
        caches[detector] = str(directory)
    return {"end": START + count * FRAME, "caches": caches}
