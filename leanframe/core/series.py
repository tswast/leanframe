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
from leanframe.core.dtypes import convert_ibis_to_pandas, convert_pandas_to_ibis


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
    def array(self) -> "pd.api.extensions.ExtensionArray":
        """Return the underlying data as a pandas ExtensionArray."""
        return self.to_pandas().array

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
        return self._data.as_table().count().to_pyarrow().as_py()

    @property
    def hasnans(self) -> bool:
        """Return True if there are any NaNs, False otherwise."""
        return self._data.isnull().any().to_pyarrow().as_py()

    @property
    def empty(self) -> bool:
        """Return True if the Series is empty, False otherwise."""
        return self.size == 0

    def __add__(self, other) -> Series:
        return Series(self._data + getattr(other, "_data", other))

    def __radd__(self, other) -> Series:
        return Series(getattr(other, "_data", other) + self._data)

    def __mul__(self, other) -> Series:
        return Series(self._data * getattr(other, "_data", other))

    def __rmul__(self, other) -> Series:
        return Series(getattr(other, "_data", other) * self._data)

    def __lt__(self, other) -> Series:
        return Series(self._data < getattr(other, "_data", other))

    def __gt__(self, other) -> Series:
        return Series(self._data > getattr(other, "_data", other))

    def __le__(self, other) -> Series:
        return Series(self._data <= getattr(other, "_data", other))

    def __ge__(self, other) -> Series:
        return Series(self._data >= getattr(other, "_data", other))

    def __ne__(self, other) -> Series:  # type: ignore[override]
        return Series(self._data != getattr(other, "_data", other))

    def __eq__(self, other) -> Series:  # type: ignore[override]
        return Series(self._data == getattr(other, "_data", other))

    def lt(self, other) -> "Series":
        """Return a boolean Series showing whether each element in the Series is less than the other."""
        return self < other

    def gt(self, other) -> "Series":
        """Return a boolean Series showing whether each element in the Series is greater than the other."""
        return self > other

    def le(self, other) -> "Series":
        """Return a boolean Series showing whether each element in the Series is less than or equal to the other."""
        return self <= other

    def ge(self, other) -> "Series":
        """Return a boolean Series showing whether each element in the Series is greater than or equal to the other."""
        return self >= other

    def ne(self, other) -> "Series":
        """Return a boolean Series showing whether each element in the Series is not equal to the other."""
        return self != other

    def eq(self, other) -> "Series":
        """Return a boolean Series showing whether each element in the Series is equal to the other."""
        return self == other

    def __round__(self, n) -> Series:
        return Series(self._data.round(n))

    def abs(self) -> "Series":
        """Return a Series with the absolute value of each element."""
        return Series(self._data.abs())

    def all(self) -> bool:
        """Return whether all elements are True."""
        return self._data.all().to_pyarrow().as_py()

    def any(self) -> bool:
        """Return whether any element is True."""
        return self._data.any().to_pyarrow().as_py()

    def sum(self):
        """Return the sum of the Series."""
        return self._data.sum().to_pyarrow().as_py()

    def mean(self):
        """Return the mean of the Series."""
        return self._data.mean().to_pyarrow().as_py()

    def min(self):
        """Return the min of the Series."""
        return self._data.min().to_pyarrow().as_py()

    def max(self):
        """Return the max of the Series."""
        return self._data.max().to_pyarrow().as_py()

    def std(self):
        """Return the std of the Series."""
        return self._data.std().to_pyarrow().as_py()

    def var(self):
        """Return the var of the Series."""
        return self._data.var().to_pyarrow().as_py()

    def count(self) -> int:
        """Return the number of non-null observations in the Series."""
        return self._data.count().to_pyarrow().as_py()

    def copy(self) -> Series:
        """Return a copy of the Series."""
        return Series(self._data)

    def isin(self, values) -> "Series":
        """Return a boolean Series showing whether each element in the Series is exactly contained in the passed sequence of values."""
        return Series(self._data.isin(values))

    def astype(self, dtype: pd.ArrowDtype) -> "Series":
        """Cast a Series to a specified dtype."""
        ibis_type = convert_pandas_to_ibis(dtype)
        return Series(self._data.cast(ibis_type))

    def to_pandas(self) -> pd.Series:
        """Convert to a pandas Series."""
        return self._data.to_pyarrow().to_pandas(
            types_mapper=lambda type_: pd.ArrowDtype(type_)
        )

    def to_numpy(self) -> np.ndarray:
        """Return a numpy representation of the Series."""
        return self.values

    def to_list(self) -> list:
        """Return a list of the values."""
        return self.to_pandas().to_list()

    def __iter__(self):
        """Return an iterator of the values."""
        return iter(self.to_list())
