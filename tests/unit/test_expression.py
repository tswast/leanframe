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

"""Tests for Expression methods."""

import pandas as pd
import pyarrow as pa
from leanframe.core.expression import Expression, col

def test_expression_comparison_methods():
    a = col('a')
    b = col('b')

    assert isinstance(a.lt(b), Expression)
    assert isinstance(a.gt(b), Expression)
    assert isinstance(a.le(b), Expression)
    assert isinstance(a.ge(b), Expression)
    assert isinstance(a.ne(b), Expression)
    assert isinstance(a.eq(b), Expression)

def test_expression_math_methods():
    a = col('a')

    assert isinstance(round(a, 2), Expression)
    assert isinstance(a.abs(), Expression)

def test_expression_aggregation_methods():
    a = col('a')

    assert isinstance(a.all(), Expression)
    assert isinstance(a.any(), Expression)
    assert isinstance(a.sum(), Expression)
    assert isinstance(a.mean(), Expression)
    assert isinstance(a.min(), Expression)
    assert isinstance(a.max(), Expression)
    assert isinstance(a.std(), Expression)
    assert isinstance(a.var(), Expression)
    assert isinstance(a.count(), Expression)

def test_expression_cumulative_methods():
    a = col('a')

    assert isinstance(a.cummax(), Expression)
    assert isinstance(a.cummin(), Expression)
    assert isinstance(a.cumprod(), Expression)
    assert isinstance(a.cumsum(), Expression)
    assert isinstance(a.diff(), Expression)

def test_expression_utility_methods():
    a = col('a')

    assert isinstance(a.isin([1, 2, 3]), Expression)
    assert isinstance(a.astype(pd.ArrowDtype(pa.int64())), Expression)
    assert isinstance(a.copy(), Expression)
