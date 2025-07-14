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
    """A 2D data structure, representing data and deferred computation."""

    def __init__(self, data):
        if isinstance(data, ibis_types.Table):
            self._data = data
        else:
            raise NotImplementedError("DataFrame constructor doesn't support local data yet.")

    @property
    def columns(self) -> pandas.Index:
        """The column labels of the DataFrame."""
        return pandas.Index(self._data.columns, dtype="object")

    def to_pandas(self) -> pandas.DataFrame:
        return self._data.to_pandas()
