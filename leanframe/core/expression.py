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
