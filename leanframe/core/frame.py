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

"""DataFrame is a two dimensional data structure."""

from __future__ import annotations

import ibis.expr.types as ibis_types
import pandas


class DataFrame:
    """A 2D data structure, representing data and deferred computation.

    WARNING: Do not call this constructor directly. Use the factory methods on
    Session, instead.
    """

    def __init__(self, data: ibis_types.Table):
        self._data = data

    @property
    def columns(self) -> pandas.Index:
        """The column labels of the DataFrame."""
        return pandas.Index(self._data.columns, dtype="object")

    def __getitem__(self, key: str):
        """Get a column.

        Note: direct row access via an Index is intentionally not implemented by
        leanframe. Check out a project like Google's BigQuery DataFrames
        (bigframes) if you require indexing.
        """
        import leanframe.core.series

        # TODO(tswast): Support filtering by a boolean Series if we get a Series
        # instead of a key? If so, the Series would have to be a column of the
        # current DataFrame, only. No joins by index key are available.
        return leanframe.core.series.Series(self._data[key])

    def to_pandas(self) -> pandas.DataFrame:
        """Convert the DataFrame to a pandas.DataFrame.

        Where possible, pandas.ArrowDtype is used to avoid lossy conversions
        from the database types to pandas.
        """
        return self._data.to_pyarrow().to_pandas(
            types_mapper=lambda type_: pandas.ArrowDtype(type_)
        )
