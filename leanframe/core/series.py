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

"""Series is a one dimensional data structure."""

from __future__ import annotations

import ibis.expr.types as ibis_types
import pandas as pd

from leanframe.core.dtypes import convert_ibis_to_pandas


class Series:
    """A 1D data structure, representing a column.

    WARNING: Do not call this constructor directly. Use the factory methods on
    Session, instead.
    """

    def __init__(self, data: ibis_types.Column):
        self._data = data

    @property
    def dtype(self) -> pd.ArrowDtype:
        """Return the dtype object of the underlying data."""
        return convert_ibis_to_pandas(self._data.type())

    @property
    def name(self) -> str:
        """Name of the column."""
        return self._data.get_name()

    def to_pandas(self) -> pd.Series:
        """Convert to a pandas Series."""
        return self._data.to_pyarrow().to_pandas(
            types_mapper=lambda type_: pd.ArrowDtype(type_)
        )
