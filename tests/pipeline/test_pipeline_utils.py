from collections import defaultdict

import pandas as pd
import pytest

from dplutils.pipeline.utils import dict_from_coord, split_dataframe


def test_dict_from_coord():
    assert dict_from_coord("a.b.c", "value") == {"a": {"b": {"c": "value"}}}


@pytest.mark.parametrize(
    "df_len, max_rows, num_splits, expected_splits",
    [
        (100, 20, None, 5),
        (101, 20, None, 6),
        (101, 21, None, 5),
        (10, 1, None, 10),
        (10, 5, None, 2),
        (10, 10, None, 1),
        (100, None, 5, 5),
        (101, None, 5, 5),
        (101, 50, 5, 5),  # this should prefer num_splits, not max
    ],
)
def test_split_dataframe(df_len, max_rows, num_splits, expected_splits):
    df = pd.DataFrame({"a": range(df_len), "b": range(df_len)})
    splits = split_dataframe(df, max_rows=max_rows, num_splits=num_splits)
    assert len(splits) == expected_splits
    assert sum(len(i) for i in splits) == df_len
    if max_rows and not num_splits:
        assert max(len(i) for i in splits) <= max_rows
    for s in splits:
        assert isinstance(s, pd.DataFrame)
        assert list(s.columns) == ["a", "b"]
    # all but one should be same length
    len_map = defaultdict(int)
    for s in splits:
        len_map[len(s)] += 1
    assert len(len_map.keys()) <= 2
    if len(len_map) > 1:
        assert 1 in len_map.values()
