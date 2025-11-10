# Copyright 2025 Google LLC, LeanFrame Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import pandas as pd
import pandas.testing
import pyarrow as pa
import pytest

import numpy as np

import leanframe


@pytest.fixture
def series_for_properties(session):
    df_pd = pd.DataFrame(
        {
            "int_col": pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())),
            "float_col": pd.Series(
                [1.0, float("nan"), 3.0], dtype=pd.ArrowDtype(pa.float64())
            ),
        }
    )
    df_lf = session.DataFrame(df_pd)
    return df_lf["int_col"], df_lf["float_col"]


@pytest.fixture
def numeric_series(session):
    df_pd = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5],
            "b": [1.1, 2.2, 3.3, 4.4, 5.5],
        }
    )
    return session.DataFrame(df_pd)


@pytest.fixture
def bool_series(session):
    df_pd = pd.DataFrame(
        {
            "all_true": [True, True, True],
            "some_true": [True, False, True],
            "all_false": [False, False, False],
        },
        dtype=pd.ArrowDtype(pa.bool_()),
    )
    return session.DataFrame(df_pd)


def test_series_ndim(series_for_properties):
    series_int, series_float = series_for_properties
    assert series_int.ndim == 1
    assert series_float.ndim == 1


def test_series_size(series_for_properties):
    series_int, series_float = series_for_properties
    assert series_int.size == 3
    assert series_float.size == 3


def test_series_shape(series_for_properties):
    series_int, series_float = series_for_properties
    assert series_int.shape == (3,)
    assert series_float.shape == (3,)


def test_series_hasnans(series_for_properties):
    series_int, series_float = series_for_properties
    assert not series_int.hasnans
    assert series_float.hasnans


def test_series_empty(session):
    df_pd = pd.DataFrame({"col1": [1, 2, 3]})
    df_lf = session.DataFrame(df_pd)
    assert not df_lf["col1"].empty

    df_pd_empty = pd.DataFrame({"col1": []})
    df_lf_empty = session.DataFrame(df_pd_empty)
    assert df_lf_empty["col1"].empty


def test_series_values(series_for_properties):
    series_int, series_float = series_for_properties
    np.testing.assert_array_equal(series_int.values, np.array([1, 2, 3]))


def test_series_array(series_for_properties):
    series_int, series_float = series_for_properties
    expected_array = pd.array([1, 2, 3], dtype=pd.ArrowDtype(pa.int64()))
    pd.testing.assert_extension_array_equal(series_int.array, expected_array)


def test_series_nbytes(series_for_properties):
    series_int, series_float = series_for_properties
    
    with pytest.raises(NotImplementedError, match="nbytes"):
        assert series_int.nbytes


@pytest.mark.parametrize(
    ("column", "expected_dtype"),
    [
        ("string_col", pd.ArrowDtype(pa.string())),
        ("int_col", pd.ArrowDtype(pa.int64())),
        ("array_col", pd.ArrowDtype(pa.list_(pa.int64()))),
        (
            "struct_col",
            pd.ArrowDtype(pa.struct([("a", pa.int64()), ("b", pa.string())])),
        ),
    ],
)
def test_series_dtype(session, column, expected_dtype):
    pa_table = pa.Table.from_pydict(
        {
            "string_col": ["a", "b", "c"],
            "int_col": [1, 2, 3],
            "array_col": [[1, 2], [3, 4], [5, 6]],
            "struct_col": [
                {"a": 1, "b": "c"},
                {"a": 2, "b": "d"},
                {"a": 3, "b": "e"},
            ],
        },
        schema=pa.schema(
            [
                pa.field("string_col", pa.string()),
                pa.field("int_col", pa.int64()),
                pa.field("array_col", pa.list_(pa.int64())),
                pa.field(
                    "struct_col",
                    pa.struct([("a", pa.int64()), ("b", pa.string())]),
                ),
            ]
        ),
    )
    pandas_df = pa_table.to_pandas(types_mapper=pd.ArrowDtype)
    df = session.DataFrame(pandas_df)
    series = df[column]
    assert series.dtype == expected_dtype


@pytest.mark.parametrize(
    ("series_pd",),
    (
        pytest.param(
            pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())),
            id="int64",
        ),
        pytest.param(
            pd.Series([1.0, float("nan"), 3.0], dtype=pd.ArrowDtype(pa.float64())),
            id="float64",
        ),
    ),
)
def test_to_pandas(session: leanframe.Session, series_pd: pd.Series):
    df_pd = pd.DataFrame(
        {
            "my_col": series_pd,
        }
    )
    df_lf = session.DataFrame(df_pd)

    result = df_lf["my_col"].to_pandas()

    # TODO(tswast): Allow input dtype != output dtype with an "expected_dtype" parameter.
    pd.testing.assert_series_equal(result, series_pd, check_names=False)


@pytest.mark.parametrize(
    ("op", "other", "expected_data"),
    [
        pytest.param(lambda s, o: s + o, 1, [2, 3, 4], id="add_scalar"),
        pytest.param(lambda s, o: o + s, 1, [2, 3, 4], id="radd_scalar"),
        pytest.param(lambda s, o: s * o, 2, [2, 4, 6], id="mul_scalar"),
        pytest.param(lambda s, o: o * s, 2, [2, 4, 6], id="rmul_scalar"),
    ],
)
def test_series_arithmetic_scalar(session, op, other, expected_data):
    pandas_df = pd.DataFrame(
        {"a": [1, 2, 3]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series_a = df["a"]

    result_series = op(series_a, other)

    expected_series = pd.Series(
        expected_data,
        dtype=pd.ArrowDtype(pa.int64()),
    )
    pd.testing.assert_series_equal(
        result_series.to_pandas(),
        expected_series,
        check_names=False,
    )


@pytest.mark.parametrize(
    ("op", "other", "expected_data"),
    [
        pytest.param(lambda s, o: s < o, 3, [True, True, False, False, False], id="lt_scalar"),
        pytest.param(lambda s, o: s.lt(o), 3, [True, True, False, False, False], id="lt_method_scalar"),
        pytest.param(lambda s, o: s > o, 3, [False, False, False, True, True], id="gt_scalar"),
        pytest.param(lambda s, o: s.gt(o), 3, [False, False, False, True, True], id="gt_method_scalar"),
        pytest.param(lambda s, o: s <= o, 3, [True, True, True, False, False], id="le_scalar"),
        pytest.param(lambda s, o: s.le(o), 3, [True, True, True, False, False], id="le_method_scalar"),
        pytest.param(lambda s, o: s >= o, 3, [False, False, True, True, True], id="ge_scalar"),
        pytest.param(lambda s, o: s.ge(o), 3, [False, False, True, True, True], id="ge_method_scalar"),
        pytest.param(lambda s, o: s != o, 3, [True, True, False, True, True], id="ne_scalar"),
        pytest.param(lambda s, o: s.ne(o), 3, [True, True, False, True, True], id="ne_method_scalar"),
        pytest.param(lambda s, o: s == o, 3, [False, False, True, False, False], id="eq_scalar"),
        pytest.param(lambda s, o: s.eq(o), 3, [False, False, True, False, False], id="eq_method_scalar"),
    ],
)
def test_series_comparison_scalar(session, op, other, expected_data):
    pandas_df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series_a = df["a"]

    result_series = op(series_a, other)

    expected_series = pd.Series(
        expected_data,
        dtype=pd.ArrowDtype(pa.bool_()),
    )
    pd.testing.assert_series_equal(
        result_series.to_pandas(),
        expected_series,
        check_names=False,
    )


@pytest.mark.parametrize(
    ("op", "expected_data"),
    [
        pytest.param(lambda s1, s2: s1 < s2, [False, False, False, True, True], id="lt_series"),
        pytest.param(lambda s1, s2: s1.lt(s2), [False, False, False, True, True], id="lt_method_series"),
        pytest.param(lambda s1, s2: s1 > s2, [True, True, False, False, False], id="gt_series"),
        pytest.param(lambda s1, s2: s1.gt(s2), [True, True, False, False, False], id="gt_method_series"),
        pytest.param(lambda s1, s2: s1 <= s2, [False, False, True, True, True], id="le_series"),
        pytest.param(lambda s1, s2: s1.le(s2), [False, False, True, True, True], id="le_method_series"),
        pytest.param(lambda s1, s2: s1 >= s2, [True, True, True, False, False], id="ge_series"),
        pytest.param(lambda s1, s2: s1.ge(s2), [True, True, True, False, False], id="ge_method_series"),
        pytest.param(lambda s1, s2: s1 != s2, [True, True, False, True, True], id="ne_series"),
        pytest.param(lambda s1, s2: s1.ne(s2), [True, True, False, True, True], id="ne_method_series"),
        pytest.param(lambda s1, s2: s1 == s2, [False, False, True, False, False], id="eq_series"),
        pytest.param(lambda s1, s2: s1.eq(s2), [False, False, True, False, False], id="eq_method_series"),
    ],
)
def test_series_comparison_series(session, op, expected_data):
    pandas_df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5], "b": [0, 1, 3, 5, 6]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series_a = df["a"]
    series_b = df["b"]

    result_series = op(series_a, series_b)

    expected_series = pd.Series(
        expected_data,
        dtype=pd.ArrowDtype(pa.bool_()),
    )
    pd.testing.assert_series_equal(
        result_series.to_pandas(),
        expected_series,
        check_names=False,
    )


def test_series_abs(session):
    pandas_df = pd.DataFrame(
        {"a": [-1, 2, -3]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series = df["a"]
    result = series.abs()
    expected = pd.Series(
        [1, 2, 3],
        name="a",
        dtype=pd.ArrowDtype(pa.int64()),
    )
    pd.testing.assert_series_equal(
        result.to_pandas(),
        expected,
        check_names=False,
    )


def test_series_cummax(session):
    pandas_df = pd.DataFrame(
        {"a": [1, 2, 3, 2, 1]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series = df["a"]
    result = series.cummax()
    expected = pd.Series(
        [1, 2, 3, 3, 3],
        name="a",
        dtype=pd.ArrowDtype(pa.int64()),
    )
    pd.testing.assert_series_equal(
        result.to_pandas(),
        expected,
        check_names=False,
    )


def test_series_cummin(session):
    pandas_df = pd.DataFrame(
        {"a": [3, 2, 1, 2, 3]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series = df["a"]
    result = series.cummin()
    expected = pd.Series(
        [3, 2, 1, 1, 1],
        name="a",
        dtype=pd.ArrowDtype(pa.int64()),
    )
    pd.testing.assert_series_equal(
        result.to_pandas(),
        expected,
        check_names=False,
    )


def test_series_cumprod(session):
    pandas_df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series = df["a"]
    result = series.cumprod()
    expected = pd.Series(
        [1, 2, 6, 24, 120],
        name="a",
        dtype=pd.ArrowDtype(pa.int64()),
    )
    pd.testing.assert_series_equal(
        result.to_pandas(),
        expected,
        check_names=False,
    )


def test_series_cumsum(session):
    pandas_df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series = df["a"]
    result = series.cumsum()
    expected = pd.Series(
        [1, 3, 6, 10, 15],
        name="a",
        dtype=pd.ArrowDtype(pa.int64()),
    )
    pd.testing.assert_series_equal(
        result.to_pandas(),
        expected,
        check_names=False,
    )


def test_series_astype(session):
    pandas_df = pd.DataFrame(
        {"a": [1, 2, 3]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series = df["a"]
    result = series.astype(pd.ArrowDtype(pa.float64()))
    expected = pd.Series(
        [1.0, 2.0, 3.0],
        name="a",
        dtype=pd.ArrowDtype(pa.float64()),
    )
    pd.testing.assert_series_equal(
        result.to_pandas(),
        expected,
        check_names=False,
    )


def test_series_describe(session):
    pandas_df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series = df["a"]
    result = series.describe()
    expected = pandas_df["a"].describe().astype("float64")
    pd.testing.assert_series_equal(
        result,
        expected,
        check_names=False,
        rtol=0.01,
    )


def test_series_diff(session):
    pandas_df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series = df["a"]
    result = series.diff()
    expected = pd.Series(
        [None, 1, 1, 1, 1],
        name="a",
        dtype=pd.ArrowDtype(pa.int64()),
    )
    pd.testing.assert_series_equal(
        result.to_pandas(),
        expected,
        check_names=False,
    )


def test_series_all(bool_series):
    assert bool_series["all_true"].all()
    assert not bool_series["some_true"].all()
    assert not bool_series["all_false"].all()


def test_series_any(bool_series):
    assert bool_series["all_true"].any()
    assert bool_series["some_true"].any()
    assert not bool_series["all_false"].any()


def test_series_round(numeric_series):
    series = numeric_series["b"]
    result = round(series, 0)
    expected = pd.Series(
        [1.0, 2.0, 3.0, 4.0, 6.0],
        dtype=pd.ArrowDtype(pa.float64()),
    )
    result_pd = result.to_pandas()
    result_pd = result_pd.astype(pd.ArrowDtype(pa.float64()))
    pd.testing.assert_series_equal(
        result_pd,
        expected,
        check_names=False,
    )


def test_series_sum(numeric_series):
    series = numeric_series["a"]
    assert series.sum() == 15


def test_series_mean(numeric_series):
    series = numeric_series["a"]
    assert series.mean() == 3.0


def test_series_min(numeric_series):
    series = numeric_series["a"]
    assert series.min() == 1


def test_series_max(numeric_series):
    series = numeric_series["a"]
    assert series.max() == 5


def test_series_std(numeric_series):
    series = numeric_series["b"]
    assert round(series.std(), 2) == 1.74


def test_series_var(numeric_series):
    series = numeric_series["b"]
    assert round(series.var(), 2) == 3.03


def test_series_count(series_for_properties):
    series_int, series_float = series_for_properties
    assert series_int.count() == 3
    assert series_float.count() == 2


def test_series_isin(session):
    pandas_df = pd.DataFrame(
        {"a": ["a", "b", "c"]},
        dtype=pd.ArrowDtype(pa.string()),
    )
    df = session.DataFrame(pandas_df)
    series = df["a"]
    result = series.isin(["a", "c"])
    expected = pd.Series(
        [True, False, True],
        name="a",
        dtype=pd.ArrowDtype(pa.bool_()),
    )
    pd.testing.assert_series_equal(
        result.to_pandas(),
        expected,
        check_names=False,
    )


def test_series_copy(session):
    df_pd = pd.DataFrame({"col1": [1, 2, 3]})
    df_lf = session.DataFrame(df_pd)
    series = df_lf["col1"]
    series_copy = series.copy()
    assert series is not series_copy
    assert series._data is series_copy._data


def test_series_to_numpy(series_for_properties):
    series_int, series_float = series_for_properties
    np.testing.assert_array_equal(series_int.to_numpy(), np.array([1, 2, 3]))


def test_series_to_list(series_for_properties):
    series_int, series_float = series_for_properties
    assert series_int.to_list() == [1, 2, 3]


def test_series_iter(series_for_properties):
    series_int, series_float = series_for_properties
    assert list(iter(series_int)) == [1, 2, 3]


@pytest.mark.parametrize(
    ("op", "expected_data"),
    [
        pytest.param(lambda s1, s2: s1 + s2, [5, 7, 9], id="add_series"),
        pytest.param(lambda s1, s2: s1 * s2, [4, 10, 18], id="mul_series"),
    ],
)
def test_series_arithmetic_series(session, op, expected_data):
    pandas_df = pd.DataFrame(
        {"a": [1, 2, 3], "b": [4, 5, 6]},
        dtype=pd.ArrowDtype(pa.int64()),
    )
    df = session.DataFrame(pandas_df)
    series_a = df["a"]
    series_b = df["b"]

    result_series = op(series_a, series_b)

    expected_series = pd.Series(
        expected_data,
        dtype=pd.ArrowDtype(pa.int64()),
    )
    pd.testing.assert_series_equal(
        result_series.to_pandas(),
        expected_series,
        check_names=False,
    )