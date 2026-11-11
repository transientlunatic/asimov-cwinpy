"""Reading segments and measuring what cwinpy heterodyned."""

import h5py
import pytest

from asimov_cwinpy import coverage


def write_segments(path, text):
    path.write_text(text)
    return str(path)


def write_output(path, segments):
    """An output file of cwinpy, with the part that records its time segments."""
    with h5py.File(path, "w") as handle:
        handle.create_dataset(coverage.SEGMENTS, data=segments)
    return str(path)


class TestReadSegments:
    def test_start_and_end_on_each_line(self, tmp_path):
        path = write_segments(tmp_path / "s.txt", "100 200\n300 450\n")
        assert coverage.read_segments(path) == [(100.0, 200.0), (300.0, 450.0)]

    def test_comments_blank_lines_and_other_text_are_ignored(self, tmp_path):
        path = write_segments(
            tmp_path / "s.txt", "# a comment\n\n100 200\nnot numbers\n# 5 6\n300 400 extra\n"
        )
        assert coverage.read_segments(path) == [(100.0, 200.0), (300.0, 400.0)]

    def test_decimals_and_exponents(self, tmp_path):
        path = write_segments(tmp_path / "s.txt", "1.5e9 1.5e9+100\n1000000000.5 1000000010.25\n")
        assert coverage.read_segments(path) == [(1000000000.5, 1000000010.25)]

    def test_a_segment_that_ends_before_it_starts_is_ignored(self, tmp_path):
        path = write_segments(tmp_path / "s.txt", "200 100\n5 5\n10 20\n")
        assert coverage.read_segments(path) == [(10.0, 20.0)]

    def test_a_file_that_is_not_there_is_an_error(self, tmp_path):
        with pytest.raises(OSError):
            coverage.read_segments(str(tmp_path / "nope.txt"))


class TestArithmetic:
    def test_overlapping_and_touching_segments_are_joined(self):
        assert coverage.union([(5, 10), (0, 3), (3, 4), (9, 12)]) == [(0, 4), (5, 12)]

    def test_a_segment_inside_another_does_not_grow_it(self):
        assert coverage.union([(0, 10), (2, 3)]) == [(0, 10)]

    def test_seconds_counts_overlaps_once(self):
        assert coverage.seconds([(0, 10), (5, 15), (20, 30)]) == 25

    def test_no_segments_is_no_time(self):
        assert coverage.seconds([]) == 0

    def test_clip_to_the_span(self):
        assert coverage.clip([(0, 10), (20, 30), (40, 50)], 5, 45) == [(5, 10), (20, 30), (40, 45)]

    def test_clip_drops_what_is_outside_or_only_touches(self):
        assert coverage.clip([(0, 5), (45, 50), (60, 70)], 5, 45) == []


class TestHeterodynedSeconds:
    def test_the_segments_of_one_file(self, tmp_path):
        path = write_output(tmp_path / "a.hdf5", [[1000.0, 1900.0], [2000.0, 2500.0]])
        assert coverage.heterodyned_seconds([path]) == 1400

    def test_the_files_of_several_jobs_are_added(self, tmp_path):
        paths = [
            write_output(tmp_path / "a.hdf5", [[0.0, 100.0]]),
            write_output(tmp_path / "b.hdf5", [[100.0, 250.0]]),
        ]
        assert coverage.heterodyned_seconds(paths) == 250

    def test_the_same_time_in_two_files_counts_once(self, tmp_path):
        paths = [
            write_output(tmp_path / "a.hdf5", [[0.0, 100.0]]),
            write_output(tmp_path / "b.hdf5", [[50.0, 150.0]]),
        ]
        assert coverage.heterodyned_seconds(paths) == 150

    def test_a_file_that_does_not_say_cannot_be_measured(self, tmp_path):
        with h5py.File(tmp_path / "x.hdf5", "w") as handle:
            handle.create_dataset("other", data=[1, 2])
        assert coverage.heterodyned_seconds([str(tmp_path / "x.hdf5")]) is None

    def test_a_file_that_is_empty_or_missing_cannot_be_measured(self, tmp_path):
        (tmp_path / "empty.hdf5").write_text("")
        assert coverage.heterodyned_seconds([str(tmp_path / "empty.hdf5")]) is None
        assert coverage.heterodyned_seconds([str(tmp_path / "missing.hdf5")]) is None

    def test_one_file_that_cannot_be_read_spoils_the_measure(self, tmp_path):
        good = write_output(tmp_path / "a.hdf5", [[0.0, 100.0]])
        assert coverage.heterodyned_seconds([good, str(tmp_path / "missing.hdf5")]) is None

    def test_without_h5py_it_cannot_be_measured(self, tmp_path, monkeypatch):
        import builtins

        real = builtins.__import__

        def refuse(name, *args, **kwargs):
            if name == "h5py":
                raise ImportError("no h5py")
            return real(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", refuse)
        path = write_output(tmp_path / "a.hdf5", [[0.0, 100.0]])
        assert coverage.heterodyned_seconds([path]) is None
