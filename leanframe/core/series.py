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
import numpy as np
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

    @property
    def values(self) -> np.ndarray:
        """Return a numpy representation of the Series."""
        return self._data.to_pyarrow().to_numpy()

    @property
    def shape(self) -> tuple[int, ...]:
        """Return a tuple of the shape of the underlying data."""
        return (self.size,)

    @property
    def nbytes(self) -> int:
        """Return the number of bytes in the underlying data."""
        raise NotImplementedError("nbytes not relevant for ibis expression.")

    @property
    def ndim(self) -> int:
        """Return the number of dimensions of the underlying data."""
        return 1

    @property
    def size(self) -> int:
        """Return the number of elements in the underlying data."""
        return self._data.count().to_pyarrow().as_py()

    @property
    def hasnans(self) -> bool:
        """Return True if there are any NaNs, False otherwise."""
        return self._data.isnull().any().to_pyarrow().as_py()
    
    def __add__(self, other) -> Series:
        return Series(self._data + getattr(other, "_data", other))
    
    def __radd__(self, other) -> Series:
        return Series(getattr(other, "_data", other) + self._data)
    
    def __mul__(self, other) -> Series:
        return Series(self._data * getattr(other, "_data", other))
    
    def __rmul__(self, other) -> Series:
        return Series(getattr(other, "_data", other) * self._data)

    def to_pandas(self) -> pd.Series:
        """Convert to a pandas Series."""
        return self._data.to_pyarrow().to_pandas(
            types_mapper=lambda type_: pd.ArrowDtype(type_)
        )
