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

"""Tests for Expression methods evaluation."""

import ibis
import pandas as pd
import pyarrow as pa
import pytest
from leanframe import Session, col


@pytest.fixture
def session():
    """Return a Session connected to an in-memory duckdb."""
    return Session(ibis.duckdb.connect())


@pytest.fixture
def df(session):
    """Return a test DataFrame."""
    data = {
        "a": [1, -2, 3, -4, 5],
        "b": [5, 4, 3, 2, 1],
    }
    return session.read_ibis(ibis.memtable(data))


def test_expression_comparison_methods(df):
    result = df.assign(
        lt=col("a").lt(col("b")),
        gt=col("a").gt(col("b")),
        le=col("a").le(col("b")),
        ge=col("a").ge(col("b")),
        ne=col("a").ne(col("b")),
        eq=col("a").eq(col("b")),
    ).to_pandas()

    pd.testing.assert_series_equal(
        result["lt"],
        pd.Series([True, True, False, True, False], name="lt"),
        check_dtype=False,
    )
    pd.testing.assert_series_equal(
        result["gt"],
        pd.Series([False, False, False, False, True], name="gt"),
        check_dtype=False,
    )
    pd.testing.assert_series_equal(
        result["le"],
        pd.Series([True, True, True, True, False], name="le"),
        check_dtype=False,
    )
    pd.testing.assert_series_equal(
        result["ge"],
        pd.Series([False, False, True, False, True], name="ge"),
        check_dtype=False,
    )
    pd.testing.assert_series_equal(
        result["ne"],
        pd.Series([True, True, False, True, True], name="ne"),
        check_dtype=False,
    )
    pd.testing.assert_series_equal(
        result["eq"],
        pd.Series([False, False, True, False, False], name="eq"),
        check_dtype=False,
    )


def test_expression_math_methods(session):
    df = session.read_ibis(ibis.memtable({"a": [1.5, -2.1, 3.8]}))
    result = df.assign(
        r=round(col("a")), r1=round(col("a"), 1), abs=col("a").abs()
    ).to_pandas()

    pd.testing.assert_series_equal(
        result["r"], pd.Series([2.0, -2.0, 4.0], name="r"), check_dtype=False
    )
    pd.testing.assert_series_equal(
        result["r1"], pd.Series([1.5, -2.1, 3.8], name="r1"), check_dtype=False
    )
    pd.testing.assert_series_equal(
        result["abs"], pd.Series([1.5, 2.1, 3.8], name="abs"), check_dtype=False
    )


def test_expression_aggregation_methods(session):
    df = session.read_ibis(ibis.memtable({"a": [1, 2, 3], "b": [True, False, True]}))
    result = df.assign(
        all_b=col("b").all(),
        any_b=col("b").any(),
        sum_a=col("a").sum(),
        mean_a=col("a").mean(),
        min_a=col("a").min(),
        max_a=col("a").max(),
        count_a=col("a").count(),
    ).to_pandas()

    pd.testing.assert_series_equal(
        result["all_b"],
        pd.Series([False, False, False], name="all_b"),
        check_dtype=False,
    )
    pd.testing.assert_series_equal(
        result["any_b"], pd.Series([True, True, True], name="any_b"), check_dtype=False
    )
    pd.testing.assert_series_equal(
        result["sum_a"], pd.Series([6, 6, 6], name="sum_a"), check_dtype=False
    )
    pd.testing.assert_series_equal(
        result["mean_a"], pd.Series([2.0, 2.0, 2.0], name="mean_a"), check_dtype=False
    )
    pd.testing.assert_series_equal(
        result["min_a"], pd.Series([1, 1, 1], name="min_a"), check_dtype=False
    )
    pd.testing.assert_series_equal(
        result["max_a"], pd.Series([3, 3, 3], name="max_a"), check_dtype=False
    )


def test_expression_cumulative_methods(session):
    df = session.read_ibis(ibis.memtable({"a": [1, 2, 3]}))
    result = df.assign(
        cummax=col("a").cummax(),
        cummin=col("a").cummin(),
        cumsum=col("a").cumsum(),
        cumprod=col("a").cumprod(),
        diff=col("a").diff(),
    ).to_pandas()

    pd.testing.assert_series_equal(
        result["cummax"], pd.Series([1, 2, 3], name="cummax"), check_dtype=False
    )
    pd.testing.assert_series_equal(
        result["cummin"], pd.Series([1, 1, 1], name="cummin"), check_dtype=False
    )
    pd.testing.assert_series_equal(
        result["cumsum"], pd.Series([1, 3, 6], name="cumsum"), check_dtype=False
    )
    # cumprod uses log trick, check approximate match. 1*1, 1*2, 2*3 = 1, 2, 6.
    pd.testing.assert_series_equal(
        result["cumprod"].round(),
        pd.Series([1.0, 2.0, 6.0], name="cumprod"),
        check_dtype=False,
    )
    pd.testing.assert_series_equal(
        result["diff"],
        pd.Series([float("nan"), 1.0, 1.0], name="diff"),
        check_dtype=False,
    )


def test_expression_utility_methods(df):
    result = df.assign(
        isin=col("a").isin([1, 3]), cast=col("a").astype(pd.ArrowDtype(pa.float64()))
    ).to_pandas()

    pd.testing.assert_series_equal(
        result["isin"],
        pd.Series([True, False, True, False, False], name="isin"),
        check_dtype=False,
    )
    pd.testing.assert_series_equal(
        result["cast"],
        pd.Series([1.0, -2.0, 3.0, -4.0, 5.0], name="cast"),
        check_dtype=False,
    )
