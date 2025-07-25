# Copyright 2023 Google LLC, LeanFrame Authors
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

"""Session manages the connection to BigQuery."""

from __future__ import annotations

import random

import ibis
import ibis.expr.types as ibis_types
import pandas


_ALPHABET = "abcdefghijklmnopqrstufwxyz"


class Session:
    """Manages a connection to an ibis backend and emulates the pandas module.

    Defaults to BigQuery.
    """

    def __init__(self, backend: ibis.BaseBackend | None):
        if backend is None:
            # TODO: add application name?
            backend = ibis.bigquery.connect()

        self._backend = backend

    def read_sql_table(self, table_name: str):
        """Create a DataFrame pointing to the table called ``table_name``."""
        import leanframe.core.frame

        return leanframe.core.frame.DataFrame(self._backend.table(table_name))

    def DataFrame(self, data: ibis_types.Table | pandas.DataFrame):
        """Construct a DataFrame."""
        import leanframe.core.frame

        if isinstance(data, ibis_types.Table):
            return leanframe.core.frame.DataFrame(data)
        elif isinstance(data, pandas.DataFrame):
            table_name = f"lf_{''.join(random.choices(_ALPHABET, k=10))}"
            table = self._backend.create_table(table_name, data, temp=True)
            return leanframe.core.frame.DataFrame(table)
        else:
            raise NotImplementedError(
                f"DataFrame constructor doesn't support {type(data)} data yet."
            )
