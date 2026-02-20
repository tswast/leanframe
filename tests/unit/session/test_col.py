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

"""Tests for Session.col."""

import ibis
import pandas as pd

import leanframe.core.expression
from leanframe.core.session import Session


def test_col_assign_with_memtable():
    """Verify col() works with assign() on a memtable based DataFrame."""
    # Use sqlite backend as dummy
    backend = ibis.sqlite.connect()
    session = Session(backend)

    # Create data
    data = pd.DataFrame({'a': [1, 2, 3]})
    t = ibis.memtable(data)
    df = session.DataFrame(t)

    # Use col to create a new column based on existing one
    deferred_col = session.col('a')

    # Verify it returns an Expression
    assert isinstance(deferred_col, leanframe.core.expression.Expression)

    # Perform arithmetic (should return another deferred Expression)
    expr = deferred_col + 1
    assert isinstance(expr, leanframe.core.expression.Expression)

    # Use in assign
    df_new = df.assign(b=expr)

    result = df_new.to_pandas()
    expected = pd.DataFrame({'a': [1, 2, 3], 'b': [2, 3, 4]})
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_col_arithmetic_chain():
    """Verify complex arithmetic chains with col()."""
    backend = ibis.sqlite.connect()
    session = Session(backend)

    col_a = session.col('a')
    col_b = session.col('b')

    # (a + b) * 2
    expr = (col_a + col_b) * 2

    data = pd.DataFrame({'a': [10], 'b': [5]})
    t = ibis.memtable(data)
    df = session.DataFrame(t)

    df_new = df.assign(c=expr)

    result = df_new.to_pandas()
    expected = pd.DataFrame({'a': [10], 'b': [5], 'c': [30]})
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
