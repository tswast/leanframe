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


def test_series_values(series_for_properties):
    series_int, series_float = series_for_properties
    np.testing.assert_array_equal(series_int.values, np.array([1, 2, 3]))

    # Older versions of numpy don't have equal_nan.
    # https://numpy.org/doc/stable/reference/generated/numpy.testing.assert_array_equal.html
    result_val = series_float.values
    expected_val = np.array([1.0, np.nan, 3.0])
    np.testing.assert_array_equal(
        np.isnan(result_val),
        np.isnan(expected_val),
    )
    np.testing.assert_array_equal(
        result_val[~np.isnan(result_val)],
        expected_val[~np.isnan(expected_val)],
    )


def test_series_nbytes(series_for_properties):
    series_int, series_float = series_for_properties
    assert series_int.nbytes > 0
    assert series_float.nbytes > 0


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