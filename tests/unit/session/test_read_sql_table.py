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

import leanframe

import pandas.testing


def test_read_sql_table_can_convert_to_pandas(session: leanframe.Session):
    """Read a table with simple scalar values."""

    df_lf = session.read_sql_table("veggies")
    df_pd = df_lf.to_pandas()

    # Make sure we have _some_ data.
    assert len(df_pd.index) > 0
    assert len(df_pd.columns) > 0

    # Where possible, check that we are compatible with pandas.
    pandas.testing.assert_index_equal(df_pd.columns, df_lf.columns)
