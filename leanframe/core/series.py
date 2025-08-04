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

import pandas
import ibis.expr.types as ibis_types


class Series:
    """A 1D data structure, representing a column.

    WARNING: Do not call this constructor directly. Use the factory methods on
    Session, instead.
    """

    def __init__(self, data: ibis_types.Column):
        self._data = data

    @property
    def name(self) -> str:
        """Name of the column."""
        return self._data.get_name()

    def to_pandas(self) -> pandas.Series:
        """Convert to a pandas Series."""
        return self._data.to_pyarrow().to_pandas(
            types_mapper=lambda type_: pandas.ArrowDtype(type_)
        )
