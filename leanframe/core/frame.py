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

import ibis
import ibis.expr.types as ibis_types
import pandas as pd

from leanframe.core.dtypes import convert_ibis_to_pandas


class DataFrame:
    """A 2D data structure, representing data and deferred computation.

    WARNING: Do not call this constructor directly. Use the factory methods on
    Session, instead.
    """

    def __init__(self, session, data: ibis_types.Table):
        self._session = session
        self._data = data

    @property
    def columns(self) -> pd.Index:
        """The column labels of the DataFrame."""
        return pd.Index(self._data.columns, dtype="object")

    @property
    def dtypes(self) -> pd.Series:
        """Return the dtypes in the DataFrame."""
        names = self._data.columns
        types = [convert_ibis_to_pandas(t) for t in self._data.schema().types]
        return pd.Series(types, index=names, name="dtypes")

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
        return leanframe.core.series.Series(self._session, self._data[key])

    def assign(self, **kwargs):
        """Assign new columns to a DataFrame.

        This corresponds to the select() method in Ibis.

        Args: 
            kwargs:
                The column names are keywords. If the values are not callable,
                (e.g. a Series, scalar, or array), they are simply assigned.
        """
        named_exprs = {
            name: self._data[name]
            for name in self._data.columns
        }
        new_exprs = {}
        for name, value in kwargs.items():
            expr = getattr(value, "_data", None)
            if expr is None:
                expr = ibis.literal(value)
            new_exprs[name] = expr
            
        named_exprs.update(new_exprs)
        return DataFrame(self._session, self._data.select(**named_exprs))

    def to_pandas(self) -> pd.DataFrame:
        """Convert the DataFrame to a pandas.DataFrame.

        Where possible, pandas.ArrowDtype is used to avoid lossy conversions
        from the database types to pandas.
        """
        return self._data.to_pyarrow().to_pandas(
            types_mapper=lambda type_: pd.ArrowDtype(type_)
        )
