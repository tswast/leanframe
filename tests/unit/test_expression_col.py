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

"""Tests for standalone col and aliases."""

import ibis
import leanframe
from leanframe.core.session import Session
from leanframe.core.expression import Expression, col

def test_col_standalone():
    """Verify col() works as a standalone function."""
    expr = col('a')
    assert isinstance(expr, Expression)

def test_col_session_static():
    """Verify Session.col() works as a static method."""
    expr = Session.col('a')
    assert isinstance(expr, Expression)

def test_col_session_instance():
    """Verify session.col() works as an instance method (compatibility)."""
    # Use sqlite backend as dummy
    backend = ibis.sqlite.connect()
    session = Session(backend)

    expr = session.col('a')
    assert isinstance(expr, Expression)

def test_col_package_alias():
    """Verify leanframe.col is available."""
    expr = leanframe.col('a')
    assert isinstance(expr, Expression)
