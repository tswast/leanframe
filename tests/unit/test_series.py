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
import pandas.testing
import pytest

import leanframe


@pytest.mark.parametrize(
    ("series_pd",),
    (
        pytest.param(
            pandas.Series([1, 2, 3]),
            id="int64",
        ),
        pytest.param(
            pandas.Series([1.0, float("nan"), 3.0]),
            id="float64",
        ),
    ),
)
def test_to_pandas(session: leanframe.Session, series_pd: pandas.Series):
    df_pd = pandas.DataFrame(
        {
            "my_col": series_pd,
        }
    )
    df_lf = session.DataFrame(df_pd)

    result = df_lf["my_col"].to_pandas()

    # TODO(tswast): Allow input dtype != output dtype with an "expected_dtype" parameter.
    pandas.testing.assert_series_equal(result, series_pd, check_names=False)
