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

import ibis
import pandas as pd
import pyarrow as pa

import leanframe
from leanframe.core.frame import DataFrame


def create_df_simple(session: leanframe.Session) -> DataFrame:
    df_pd = pd.DataFrame(
        {
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
            "col3": [1.1, 2.2, 3.3],
        }
    ).astype(
        {
            "col1": pd.ArrowDtype(pa.int64()),
            "col2": pd.ArrowDtype(pa.string()),
            "col3": pd.ArrowDtype(pa.float64()),
        }
    )
    df_lf = session.DataFrame(df_pd)
    return df_lf


def create_df_complex(session: leanframe.Session) -> DataFrame:
    pa_table = pa.Table.from_pydict(
        {
            "array_col": [[1, 2], [3, 4]],
            "struct_col": [{"a": 1, "b": "c"}, {"a": 2, "b": "d"}],
        },
        schema=pa.schema(
            [
                pa.field("array_col", pa.list_(pa.int64())),
                pa.field(
                    "struct_col",
                    pa.struct([("a", pa.int64()), ("b", pa.string())]),
                ),
            ]
        ),
    )
    df_pd = pa_table.to_pandas(types_mapper=pd.ArrowDtype)
    df_lf = session.DataFrame(df_pd)
    return df_lf

if __name__ == "__main__":
    backend = ibis.duckdb.connect()
    session = leanframe.Session(backend=backend)
    df_simple = create_df_simple(session)
    df_complex = create_df_complex(session)
    pass
