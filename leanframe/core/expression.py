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

"""Expression represents a deferred computation."""

from __future__ import annotations

import ibis
import ibis.expr.types as ibis_types
import pandas as pd
from leanframe.core.dtypes import convert_pandas_to_ibis


def col(name: str) -> Expression:
    """Return a new expression object which is a deferred series."""
    return Expression(ibis.deferred[name])


class Expression:
    """A wrapper around an Ibis expression for deferred computation."""

    def __init__(self, data: ibis_types.Value):
        self._data = data

    def __add__(self, other) -> Expression:
        return Expression(self._data + getattr(other, "_data", other))

    def __radd__(self, other) -> Expression:
        return Expression(getattr(other, "_data", other) + self._data)

    def __mul__(self, other) -> Expression:
        return Expression(self._data * getattr(other, "_data", other))

    def __rmul__(self, other) -> Expression:
        return Expression(getattr(other, "_data", other) * self._data)

    def __lt__(self, other) -> Expression:
        return Expression(self._data < getattr(other, "_data", other))

    def __gt__(self, other) -> Expression:
        return Expression(self._data > getattr(other, "_data", other))

    def __le__(self, other) -> Expression:
        return Expression(self._data <= getattr(other, "_data", other))

    def __ge__(self, other) -> Expression:
        return Expression(self._data >= getattr(other, "_data", other))

    def __ne__(self, other) -> Expression:  # type: ignore[override]
        return Expression(self._data != getattr(other, "_data", other))

    def __eq__(self, other) -> Expression:  # type: ignore[override]
        return Expression(self._data == getattr(other, "_data", other))

    def lt(self, other) -> Expression:
        """Return a boolean Expression showing whether each element is less than the other."""
        return self < other

    def gt(self, other) -> Expression:
        """Return a boolean Expression showing whether each element is greater than the other."""
        return self > other

    def le(self, other) -> Expression:
        """Return a boolean Expression showing whether each element is less than or equal to the other."""
        return self <= other

    def ge(self, other) -> Expression:
        """Return a boolean Expression showing whether each element is greater than or equal to the other."""
        return self >= other

    def ne(self, other) -> Expression:
        """Return a boolean Expression showing whether each element is not equal to the other."""
        return self != other

    def eq(self, other) -> Expression:
        """Return a boolean Expression showing whether each element is equal to the other."""
        return self == other

    def __round__(self, n=0) -> Expression:
        return Expression(self._data.round(n))

    def abs(self) -> Expression:
        """Return an Expression with the absolute value of each element."""
        return Expression(self._data.abs())

    def all(self) -> Expression:
        """Return whether all elements are True."""
        return Expression(self._data.all())

    def any(self) -> Expression:
        """Return whether any element is True."""
        return Expression(self._data.any())

    def sum(self) -> Expression:
        """Return the sum of the Expression."""
        return Expression(self._data.sum())

    def mean(self) -> Expression:
        """Return the mean of the Expression."""
        return Expression(self._data.mean())

    def min(self) -> Expression:
        """Return the min of the Expression."""
        return Expression(self._data.min())

    def max(self) -> Expression:
        """Return the max of the Expression."""
        return Expression(self._data.max())

    def std(self) -> Expression:
        """Return the std of the Expression."""
        return Expression(self._data.std())

    def var(self) -> Expression:
        """Return the var of the Expression."""
        return Expression(self._data.var())

    def count(self) -> Expression:
        """Return the number of non-null observations in the Expression."""
        return Expression(self._data.count())

    def cummax(self) -> Expression:
        """Return an Expression with the cumulative maximum of each element."""
        return Expression(self._data.cummax())

    def cummin(self) -> Expression:
        """Return an Expression with the cumulative minimum of each element."""
        return Expression(self._data.cummin())

    def cumprod(self) -> Expression:
        """Return an Expression with the cumulative product of each element.

        Note: This currently uses a `log().cumsum().exp()` workaround, which
        may fail or return NaN if the data contains zeros or negative numbers.
        """
        return Expression(self._data.log().cumsum().exp().cast(self._data.type()))

    def cumsum(self) -> Expression:
        """Return an Expression with the cumulative sum of each element."""
        return Expression(self._data.cumsum())

    def diff(self) -> Expression:
        """Return an Expression with the difference between each element and the previous element."""
        return Expression(self._data - self._data.lag())

    def copy(self) -> Expression:
        """Return a copy of the Expression."""
        return Expression(self._data)

    def isin(self, values) -> Expression:
        """Return a boolean Expression showing whether each element is contained in values."""
        return Expression(self._data.isin(values))

    def astype(self, dtype: pd.ArrowDtype) -> Expression:
        """Cast an Expression to a specified dtype."""
        ibis_type = convert_pandas_to_ibis(dtype)
        return Expression(self._data.cast(ibis_type))
