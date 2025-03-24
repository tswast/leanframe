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

from __future__ import annotations

import functools
import typing

import ibis
import ibis.common.exceptions
import ibis.expr.datatypes as ibis_dtypes
import ibis.expr.operations.generic
import ibis.expr.types as ibis_types
import numpy as np

import leanframe.dtypes

_ZERO = typing.cast(ibis_types.NumericValue, ibis_types.literal(0))
_INF = typing.cast(ibis_types.NumericValue, ibis_types.literal(np.inf))

BinaryOp = typing.Callable[[ibis_types.Value, ibis_types.Value], ibis_types.Value]
TernaryOp = typing.Callable[
    [ibis_types.Value, ibis_types.Value, ibis_types.Value], ibis_types.Value
]


### Unary Ops
class UnaryOp:
    def _as_ibis(self, x):
        raise NotImplementedError("Base class UnaryOp has no implementation.")

    @property
    def is_windowed(self):
        return False


class AbsOp(UnaryOp):
    def _as_ibis(self, x: ibis_types.Value):
        return typing.cast(ibis_types.NumericValue, x).abs()


class InvertOp(UnaryOp):
    def _as_ibis(self, x: ibis_types.Value):
        return typing.cast(ibis_types.NumericValue, x).negate()


class IsNullOp(UnaryOp):
    def _as_ibis(self, x: ibis_types.Value):
        return x.isnull()


class LenOp(UnaryOp):
    def _as_ibis(self, x: ibis_types.Value):
        return typing.cast(ibis_types.StringValue, x).length()


class NotNullOp(UnaryOp):
    def _as_ibis(self, x: ibis_types.Value):
        return x.notnull()


class ReverseOp(UnaryOp):
    def _as_ibis(self, x: ibis_types.Value):
        return typing.cast(ibis_types.StringValue, x).reverse()


class LowerOp(UnaryOp):
    def _as_ibis(self, x: ibis_types.Value):
        return typing.cast(ibis_types.StringValue, x).lower()


class UpperOp(UnaryOp):
    def _as_ibis(self, x: ibis_types.Value):
        return typing.cast(ibis_types.StringValue, x).upper()


class StripOp(UnaryOp):
    def _as_ibis(self, x: ibis_types.Value):
        return typing.cast(ibis_types.StringValue, x).strip()


class IsNumericOp(UnaryOp):
    def _as_ibis(self, x: ibis_types.Value):
        # catches all members of the Unicode number class, which matches pandas isnumeric
        # see https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_contains
        return typing.cast(ibis_types.StringValue, x).re_search(r"^(\pN*)$")


class RstripOp(UnaryOp):
    def _as_ibis(self, x: ibis_types.Value):
        return typing.cast(ibis_types.StringValue, x).rstrip()


# Parameterized ops
class AsTypeOp(UnaryOp):
    def __init__(self, to_type: leanframe.dtypes.IbisDtype):
        self.to_type = to_type

    def _as_ibis(self, x: ibis_types.Value):
        return leanframe.dtypes.cast_ibis_value(x, self.to_type)


class FindOp(UnaryOp):
    def __init__(self, sub, start, end):
        self._sub = sub
        self._start = start
        self._end = end

    def _as_ibis(self, x: ibis_types.Value):
        return typing.cast(ibis_types.StringValue, x).find(
            self._sub, self._start, self._end
        )


class SliceOp(UnaryOp):
    def __init__(self, start, stop):
        self._start = start
        self._stop = stop

    def _as_ibis(self, x: ibis_types.Value):
        return typing.cast(ibis_types.StringValue, x)[self._start : self._stop]


class BinopPartialRight(UnaryOp):
    def __init__(self, binop: BinaryOp, right_scalar: typing.Any):
        self._binop = binop
        self._right = right_scalar

    def _as_ibis(self, x):
        return self._binop(x, self._right)


class BinopPartialLeft(UnaryOp):
    def __init__(self, binop: BinaryOp, left_scalar: typing.Any):
        self._binop = binop
        self._left = left_scalar

    def _as_ibis(self, x):
        return self._binop(self._left, x)


abs_op = AbsOp()
invert_op = InvertOp()
isnull_op = IsNullOp()
len_op = LenOp()
notnull_op = NotNullOp()
reverse_op = ReverseOp()
lower_op = LowerOp()
upper_op = UpperOp()
strip_op = StripOp()
isnumeric_op = IsNumericOp()
rstrip_op = RstripOp()


### Binary Ops
def short_circuit_nulls(type_override: typing.Optional[ibis_dtypes.DataType] = None):
    """Wraps a binary operator to generate nulls of the expected type if either input is a null scalar."""

    def short_circuit_nulls_inner(binop):
        @functools.wraps(binop)
        def wrapped_binop(x: ibis_types.Value, y: ibis_types.Value):
            if isinstance(x, ibis_types.NullScalar):
                return ibis_types.null().cast(type_override or y.type())
            elif isinstance(y, ibis_types.NullScalar):
                return ibis_types.null().cast(type_override or x.type())
            else:
                return binop(x, y)

        return wrapped_binop

    return short_circuit_nulls_inner


def and_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    return typing.cast(ibis_types.BooleanValue, x) & typing.cast(
        ibis_types.BooleanValue, y
    )


def or_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    return typing.cast(ibis_types.BooleanValue, x) | typing.cast(
        ibis_types.BooleanValue, y
    )


@short_circuit_nulls()
def add_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    if isinstance(x, ibis_types.NullScalar) or isinstance(x, ibis_types.NullScalar):
        return
    return typing.cast(ibis_types.NumericValue, x) + typing.cast(
        ibis_types.NumericValue, y
    )


@short_circuit_nulls()
def sub_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    return typing.cast(ibis_types.NumericValue, x) - typing.cast(
        ibis_types.NumericValue, y
    )


@short_circuit_nulls()
def mul_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    return typing.cast(ibis_types.NumericValue, x) * typing.cast(
        ibis_types.NumericValue, y
    )


@short_circuit_nulls(ibis_dtypes.float)
def div_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    return typing.cast(ibis_types.NumericValue, x) / typing.cast(
        ibis_types.NumericValue, y
    )


def eq_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    return x.__eq__(y)


@short_circuit_nulls(ibis_dtypes.bool)
def lt_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    return x < y


@short_circuit_nulls(ibis_dtypes.bool)
def le_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    return x <= y


@short_circuit_nulls(ibis_dtypes.bool)
def gt_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    return x > y


@short_circuit_nulls(ibis_dtypes.bool)
def ge_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    return x >= y


@short_circuit_nulls(ibis_dtypes.int)
def floordiv_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    x_numeric = typing.cast(ibis_types.NumericValue, x)
    y_numeric = typing.cast(ibis_types.NumericValue, y)
    floordiv_expr = x_numeric // y_numeric

    # DIV(N, 0) will error in bigquery, but needs to return 0 for int, and inf for float in BQ so we short-circuit in this case.
    # Multiplying left by zero propogates nulls.
    zero_result = _INF if (x.type().is_floating() or y.type().is_floating()) else _ZERO
    return (
        ibis.case()
        .when(y_numeric == _ZERO, zero_result * x_numeric)
        .else_(floordiv_expr)
        .end()
    )


@short_circuit_nulls()
def mod_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    # TODO(tbergeron): fully support floats, including when mixed with integer
    # Pandas has inconsitency about whether N mod 0. Most conventions have this be NAN.
    # For some dtypes, the result is 0 instead. This implementation results in NA always.
    x_numeric = typing.cast(ibis_types.NumericValue, x)
    y_numeric = typing.cast(ibis_types.NumericValue, y)
    # Hacky short-circuit to avoid passing zero-literal to sql backend, evaluate locally instead to null.
    op = y.op()
    if isinstance(op, ibis.expr.operations.generic.Literal) and op.value == 0:
        return ibis_types.null().cast(x.type())

    bq_mod = x_numeric % y_numeric  # Bigquery will maintain x sign here
    # In BigQuery returned value has the same sign as X. In pandas, the sign of y is used, so we need to flip the result if sign(x) != sign(y)
    return (
        ibis.case()
        .when(
            y_numeric == _ZERO, _ZERO * x_numeric
        )  # Dummy op to propogate nulls and type from x arg
        .when(
            (y_numeric < _ZERO) & (bq_mod > _ZERO), (y_numeric + bq_mod)
        )  # Convert positive result to negative
        .when(
            (y_numeric > _ZERO) & (bq_mod < _ZERO), (y_numeric + bq_mod)
        )  # Convert negative result to positive
        .else_(bq_mod)
        .end()
    )


def fillna_op(
    x: ibis_types.Value,
    y: ibis_types.Value,
):
    return x.fillna(typing.cast(ibis_types.Scalar, y))


def reverse(op: BinaryOp) -> BinaryOp:
    return lambda x, y: op(y, x)


def partial_left(op: BinaryOp, scalar: typing.Any) -> UnaryOp:
    return BinopPartialLeft(op, scalar)


def partial_right(op: BinaryOp, scalar: typing.Any) -> UnaryOp:
    return BinopPartialRight(op, scalar)


# Ternary ops
def where_op(
    original: ibis_types.Value,
    condition: ibis_types.Value,
    replacement: ibis_types.Value,
) -> ibis_types.Value:
    """Returns x if y is true, otherwise returns z."""
    return ibis.case().when(condition, original).else_(replacement).end()


def clip_op(
    original: ibis_types.Value,
    lower: ibis_types.Value,
    upper: ibis_types.Value,
) -> ibis_types.Value:
    """Clips value to lower and upper bounds."""
    if isinstance(lower, ibis_types.NullScalar) and (
        not isinstance(upper, ibis_types.NullScalar)
    ):
        return (
            ibis.case()
            .when(upper.isnull() | (original > upper), upper)
            .else_(original)
            .end()
        )
    elif (not isinstance(lower, ibis_types.NullScalar)) and isinstance(
        upper, ibis_types.NullScalar
    ):
        return (
            ibis.case()
            .when(lower.isnull() | (original < lower), lower)
            .else_(original)
            .end()
        )
    elif isinstance(lower, ibis_types.NullScalar) and (
        isinstance(upper, ibis_types.NullScalar)
    ):
        return original
    else:
        # Note: Pandas has unchanged behavior when upper bound and lower bound are flipped. This implementation requires that lower_bound < upper_bound
        return (
            ibis.case()
            .when(lower.isnull() | (original < lower), lower)
            .when(upper.isnull() | (original > upper), upper)
            .else_(original)
            .end()
        )
