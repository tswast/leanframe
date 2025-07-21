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

import pandas

import leanframe
import leanframe.core.series


def test_dataframe_getitem_with_column(session: leanframe.Session):
    """Read a table with simple scalar values."""

    df_lf = session.DataFrame(
        pandas.DataFrame(
            {
                "col1": [1, 2, 3],
                "col2": ["a", "b", "c"],
            }
        )
    )
    series_1 = df_lf["col1"]
    assert isinstance(series_1, leanframe.core.series.Series)
    assert series_1.name == "col1"
    # TODO(tswast): check dtype

    series_2 = df_lf["col2"]
    assert isinstance(series_2, leanframe.core.series.Series)
    assert series_2.name == "col2"
    # TODO(tswast): check dtype
