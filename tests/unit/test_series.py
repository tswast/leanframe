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

import leanframe


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
