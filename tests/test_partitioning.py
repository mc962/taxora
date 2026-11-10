from datetime import date

from taxora.partitioning import split_window, window_from_range, window_to_range


def test_split_window_halves_are_contiguous_and_cover_original():
    start, end = date(2020, 1, 1), date(2020, 12, 31)
    left, right = split_window((start, end))
    assert left[0] == start and right[1] == end
    assert (right[0] - left[1]).days == 1
    assert left[0] <= left[1] and right[0] <= right[1]


def test_split_window_two_days():
    assert split_window((date(2020, 1, 1), date(2020, 1, 2))) == (
        (date(2020, 1, 1), date(2020, 1, 1)),
        (date(2020, 1, 2), date(2020, 1, 2)),
    )


def test_split_window_single_day_cannot_split():
    assert split_window((date(2020, 1, 1), date(2020, 1, 1))) is None


def test_range_round_trip():
    w = (date(2020, 1, 1), date(2020, 6, 30))
    assert window_from_range(window_to_range(w)) == w
    assert window_from_range("2020-03-04") == (date(2020, 3, 4), date(2020, 3, 4))
