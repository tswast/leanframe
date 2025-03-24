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

"""DataFrame is a two dimensional data structure."""

from __future__ import annotations

import re
import typing
from typing import (
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import google.cloud.bigquery as bigquery
import ibis
import ibis.expr.datatypes as ibis_dtypes
import ibis.expr.types as ibis_types
import pandas as pd

import leanframe.aggregations as agg_ops
import leanframe.core
import leanframe.core.blocks as blocks
import leanframe.core.groupby as groupby
import leanframe.core.indexes as indexes
import leanframe.core.joins as joins
import leanframe.core.ordering as order
import leanframe.dtypes
import leanframe.operations as ops
import leanframe.series
import third_party.leanframe_vendored.pandas.pandas.io.common as vendored_pandas_io_common


class DataFrame:
    """A 2D data structure, representing data and deferred computation.

    .. warning::
        This constructor is **private**. Use a public method such as
        ``Session.read_gbq`` to construct a DataFrame.
    """

    def __init__(
        self,
        index: indexes.ImplicitJoiner,
        columns: Optional[Sequence[str]] = None,
    ):
        self._index = index
        self._block = index._block
        # One on one match between BF column names and real value column names in BQ SQL.
        self._col_labels = list(columns) if columns else list(self._block.value_columns)

    def _ipython_key_completions_(self) -> List[str]:
        return list(self._col_labels)

    def _copy(
        self, columns: Optional[Tuple[Sequence[ibis_types.Value], Sequence[str]]] = None
    ) -> DataFrame:
        if not columns:
            return DataFrame(self._block.copy().index, columns=self._col_labels)

        value_cols, col_labels = columns
        if len(value_cols) != len(col_labels):
            raise ValueError(
                f"Column sizes not equal. Value columns size: {len(value_cols)}, column names size: {len(col_labels)}"
            )

        block = self._block.copy(value_cols)
        index = self._recreate_index(block)
        return DataFrame(index, col_labels)

    @property
    def dtypes(self) -> pd.Series:
        """Returns the dtypes as a Pandas Series object"""
        return pd.Series(data=self._block.dtypes, index=self._col_labels)

    @property
    def columns(self) -> pd.Index:
        """Returns the column labels of the dataframe"""
        return self.dtypes.index

    @property
    def shape(self) -> Tuple[int, int]:
        """Return a tuple representing the dimensionality of the DataFrame."""
        block_length, _ = self._block.expr.shape()
        return (block_length, len(self.columns))

    @property
    def size(self) -> int:
        """The size of the dataframe, defined as the number of rows times the number of columns."""
        rows, cols = self.shape
        return rows * cols

    @property
    def ndim(self) -> int:
        """Number of dimensions. Always 2 for dataframe."""
        return 2

    @property
    def empty(self) -> bool:
        """Whether the dataframe is entirely empty with 0 columns and 0 rows"""
        return not bool(self._block.value_columns)

    def __getitem__(
        self, key: Union[str, Sequence[str], leanframe.series.Series]
    ) -> Union[leanframe.series.Series, "DataFrame"]:
        """Gets the specified column(s) from the DataFrame."""
        # NOTE: This implements the operations described in
        # https://pandas.pydata.org/docs/getting_started/intro_tutorials/03_subset_data.html

        if isinstance(key, leanframe.series.Series):
            return self._getitem_bool_series(key)

        sql_names = self._sql_names(key)
        # Only input is a str and only find one column, returns a Series
        if isinstance(key, str) and len(sql_names) == 1:
            sql_name = sql_names[0]
            # Check that the column exists.
            # TODO(swast): Make sure we can't select Index columns this way.
            index_exprs = [
                self._block.expr.get_column(index_key)
                for index_key in self._block.index_columns
            ]
            series_expr = self._block.expr.get_column(sql_name)
            # Copy to emulate "Copy-on-Write" semantics.
            block = self._block.copy()
            # Since we're working with a "copy", we can drop the other value
            # columns. They aren't needed on the Series.
            block.expr = block.expr.projection(index_exprs + [series_expr])
            block.index.name = self._block.index.name
            # key can only be str or [str] with 1 item when sql_names only has 1 item
            series_name = key if isinstance(key, str) else key[0]
            return leanframe.series.Series(block, sql_name, name=series_name)

        # Select a subset of columns or re-order columns.
        # In Ibis after you apply a projection, any column objects from the
        # table before the projection can't be combined with column objects
        # from the table after the projection. This is because the table after
        # a projection is considered a totally separate table expression.
        #
        # This is unexpected behavior for a pandas user, who expects their old
        # Series objects to still work with the new / mutated DataFrame. We
        # avoid applying a projection in Ibis until it's absolutely necessary
        # to provide pandas-like semantics.
        # TODO(swast): Do we need to apply implicit join when doing a
        # projection?

        # Select a number of columns as DF.
        key = [key] if isinstance(key, str) else key

        value_cols = self._block.get_value_col_exprs(sql_names)
        col_label_count: Dict[str, int] = {}
        for col_label in self._col_labels:
            col_label_count[col_label] = col_label_count.get(col_label, 0) + 1
        col_labels = []
        for item_name in key:
            # Every item is guaranteed to exist, otherwise it already raised exception in sql_names.
            col_labels += [item_name] * col_label_count[item_name]

        return self._copy((value_cols, col_labels))

    # Bool Series selects rows
    def _getitem_bool_series(self, key: leanframe.series.Series) -> DataFrame:
        if not key._to_ibis_expr().type() == ibis_dtypes.bool:
            raise ValueError("Only boolean series currently supported for indexing.")
            # TODO: enforce stricter alignment
        combined_index, (
            get_column_left,
            get_column_right,
        ) = self._block.index.join(key.index, how="left")
        block = combined_index._block
        block.replace_value_columns(
            [
                get_column_left(left_col).name(left_col)
                for left_col in self._block.value_columns
            ]
        )
        right = get_column_right(key._value_column)

        block.expr = block.expr.filter((right == ibis.literal(True)))
        return DataFrame(combined_index, self._col_labels)

    def __repr__(self) -> str:
        """Converts a DataFrame to a string. Calls compute. Only represents the first 25 results."""
        max_results = 25
        pandas_df, row_count = self._retrieve_repr_request_results(max_results)
        column_count = len(pandas_df.columns)

        # Modify the end of the string to reflect count.
        repr_string = repr(pandas_df)
        lines = repr_string.split("\n")
        pattern = re.compile("\\[[0-9]+ rows x [0-9]+ columns\\]")
        if pattern.match(lines[-1]):
            lines = lines[:-2]

        if row_count > len(lines) - 1:
            lines.append("...")

        lines.append("")
        lines.append(f"[{row_count} rows x {column_count} columns]")
        return "\n".join(lines)

    def _repr_html_(self) -> str:
        """
        Returns an html string primarily for use by notebooks for displaying
        a representation of the DataFrame. Displays 20 rows by default since
        many notebooks are not configured for large tables.
        """
        max_results = 20
        pandas_df, row_count = self._retrieve_repr_request_results(max_results)
        column_count = len(pandas_df.columns)
        # _repr_html_ stub is missing so mypy thinks it's a Series. Ignore mypy.
        html_string = pandas_df._repr_html_()  # type:ignore
        html_string += f"[{row_count} rows x {column_count} columns in total]"
        return html_string

    def _retrieve_repr_request_results(
        self, max_results: Optional[int]
    ) -> Tuple[pd.DataFrame, int]:
        """
        Retrieves a pandas dataframe containing only max_results many rows for use with printing methods.

        Returns a tuple of the dataframe and the overall number of rows of the query.
        """
        computed_df, count = self._block._compute_and_count(max_results=max_results)
        formatted_df = computed_df.set_axis(self._col_labels, axis=1)
        # we reset the axis and substitute the bf index name for the default
        formatted_df.index.name = self.index.name
        return formatted_df, count

    def _apply_binop(
        self,
        other: float | int | leanframe.Series,
        op,
        reverse: bool = False,
        axis: str | int = "columns",
    ):
        if isinstance(other, (float, int)):
            return self._apply_scalar_binop(other, op, reverse=reverse)
        elif isinstance(other, leanframe.Series):
            return self._apply_series_binop(other, op, reverse=reverse, axis=axis)

    def _apply_scalar_binop(
        self, other: float | int, op, reverse: bool = False
    ) -> DataFrame:
        scalar = leanframe.dtypes.literal_to_ibis_scalar(other)
        value_cols = []
        for value_col in self._block.get_value_col_exprs():
            value_cols.append(
                (op(scalar, value_col) if reverse else op(value_col, scalar)).name(
                    value_col.get_name()
                )
            )
        return self._copy((value_cols, self._col_labels))

    def _apply_series_binop(
        self,
        other: leanframe.Series,
        op,
        reverse: bool = False,
        axis: str | int = "columns",
    ) -> DataFrame:
        if axis not in ("columns", "index", 0, 1):
            raise ValueError(f"Invalid input: axis {axis}.")

        if axis in ("columns", 1):
            raise NotImplementedError("Row Series operations haven't been supported.")

        joined_index, (get_column_left, get_column_right) = self.index.join(
            other.index, how="outer"
        )
        joined_index.name = self._index.name

        series_column_id = other._value.get_name()
        series_col = get_column_right(series_column_id)
        value_cols = []
        for column_id in self._block.value_columns:
            value_col = get_column_left(column_id)
            value_cols.append(
                (
                    op(series_col, value_col) if reverse else op(value_col, series_col)
                ).name(value_col.get_name())
            )

        block = joined_index._block
        block.replace_value_columns(value_cols)
        return DataFrame(joined_index, self._col_labels)

    def add(
        self, other: float | int | leanframe.Series, axis: str | int = "columns"
    ) -> DataFrame:
        """Add dataframe and other element-wise."""
        return self._apply_binop(other, ops.add_op, axis=axis)

    __radd__ = __add__ = radd = add

    def sub(
        self, other: float | int | leanframe.Series, axis: str | int = "columns"
    ) -> DataFrame:
        """Subtract other from dataframe element-wise."""
        return self._apply_binop(other, ops.sub_op, axis=axis)

    __sub__ = sub

    def rsub(
        self, other: float | int | leanframe.Series, axis: str | int = "columns"
    ) -> DataFrame:
        """Subtract dataframe from other element-wise."""
        return self._apply_binop(other, ops.sub_op, reverse=True, axis=axis)

    __rsub__ = rsub

    def mul(
        self, other: float | int | leanframe.Series, axis: str | int = "columns"
    ) -> DataFrame:
        """Multiply dataframe and other element-wise."""
        return self._apply_binop(other, ops.mul_op, axis=axis)

    __rmul__ = __mul__ = rmul = mul

    def truediv(
        self, other: float | int | leanframe.Series, axis: str | int = "columns"
    ) -> DataFrame:
        """Divide dataframe by other element-wise."""
        return self._apply_binop(other, ops.div_op, axis=axis)

    div = __truediv__ = truediv

    def rtruediv(
        self, other: float | int | leanframe.Series, axis: str | int = "columns"
    ) -> DataFrame:
        """Divide other by dataframe element-wise."""
        return self._apply_binop(other, ops.div_op, reverse=True, axis=axis)

    __rtruediv__ = rdiv = rtruediv

    def floordiv(
        self, other: float | int | leanframe.Series, axis: str | int = "columns"
    ) -> DataFrame:
        """Divide dataframe by other element-wise, rounding down to the next integer."""
        return self._apply_binop(other, ops.floordiv_op, axis=axis)

    __floordiv__ = floordiv

    def rfloordiv(
        self, other: float | int | leanframe.Series, axis: str | int = "columns"
    ) -> DataFrame:
        """Divide other by dataframe element-wise, rounding down to the next integer."""
        return self._apply_binop(other, ops.floordiv_op, reverse=True, axis=axis)

    __rfloordiv__ = rfloordiv

    def lt(self, other: typing.Any, axis: str | int = "columns") -> DataFrame:
        return self._apply_binop(other, ops.lt_op, axis=axis)

    def le(self, other: typing.Any, axis: str | int = "columns") -> DataFrame:
        return self._apply_binop(other, ops.le_op, axis=axis)

    def gt(self, other: typing.Any, axis: str | int = "columns") -> DataFrame:
        return self._apply_binop(other, ops.gt_op, axis=axis)

    def ge(self, other: typing.Any, axis: str | int = "columns") -> DataFrame:
        return self._apply_binop(other, ops.ge_op, axis=axis)

    __lt__ = lt

    __le__ = le

    __gt__ = gt

    __ge__ = ge

    def mod(self, other: int | leanframe.Series, axis: str | int = "columns") -> DataFrame:  # type: ignore
        return self._apply_binop(other, ops.mod_op, axis=axis)

    def rmod(self, other: int | leanframe.Series, axis: str | int = "columns") -> DataFrame:  # type: ignore
        return self._apply_binop(other, ops.reverse(ops.mod_op), axis=axis)

    __mod__ = mod

    __rmod__ = rmod

    def compute(self) -> pd.DataFrame:
        """Executes deferred operations and downloads the results."""
        df = self._block.compute()
        return df.set_axis(self._col_labels, axis=1)

    def copy(self) -> DataFrame:
        """Creates a deep copy of the DataFrame."""
        return self._copy()

    def head(self, n: int = 5) -> DataFrame:
        """Limits DataFrame to a specific number of rows."""
        df = self._copy()
        df._block.expr = self._block.expr.apply_limit(n)
        # TODO(swast): Why isn't the name sticking?
        df.index.name = self.index.name
        return df

    def drop(self, *, columns: Union[str, Iterable[str]]) -> DataFrame:
        """Drop specified column(s)."""
        if isinstance(columns, str):
            columns = [columns]
        columns = list(columns)

        df = self._copy()
        df._block.expr = self._block.expr.drop_columns(self._sql_names(columns))
        df._col_labels = [
            col_label for col_label in self._col_labels if col_label not in columns
        ]
        return df

    def rename(self, *, columns: Mapping[str, str]) -> DataFrame:
        """Alter column labels."""
        # TODO(garrettwu) Support function(Callable) as columns parameter.
        col_labels = [
            (columns.get(col_label, col_label)) for col_label in self._col_labels
        ]
        return self._copy((self._block.get_value_col_exprs(), col_labels))

    def assign(self, **kwargs) -> DataFrame:
        """Assign new columns to a DataFrame.

        Returns a new object with all original columns in addition to new ones. Existing columns that are re-assigned will be overwritten.
        """
        # TODO(garrettwu) Support list-like values. Requires ordering.
        # TODO(garrettwu) Support callable values.

        cur = self
        for k, v in kwargs.items():
            cur = cur._assign_single_item(k, v)

        return cur

    def _assign_single_item(
        self, k: str, v: Union[leanframe.series.Series, int, float]
    ) -> DataFrame:
        if isinstance(v, leanframe.series.Series):
            return self._assign_series_join_on_index(k, v)
        else:
            return self._assign_scalar(k, v)

    def _assign_scalar(self, k: str, v: Union[int, float]) -> DataFrame:
        scalar = leanframe.dtypes.literal_to_ibis_scalar(v)
        # TODO(swast): Make sure that k is the ID / SQL name, not a label,
        # which could be invalid SQL.
        scalar = scalar.name(k)

        value_cols = self._block.get_value_col_exprs()
        col_labels = list(self._col_labels)

        sql_names = self._sql_names(k, tolerance=True)
        if sql_names:
            for i, value_col in enumerate(value_cols):
                if value_col.get_name() in sql_names:
                    value_cols[i] = scalar
        else:
            # TODO(garrettwu): Make sure sql name won't conflict.
            value_cols.append(scalar)
            col_labels.append(k)

        return self._copy((value_cols, col_labels))

    def _assign_series_join_on_index(self, k, v: leanframe.series.Series) -> DataFrame:
        joined_index, (get_column_left, get_column_right) = self.index.join(
            v.index, how="left"
        )
        joined_index.name = self._index.name

        sql_names = self._sql_names(k, tolerance=True)
        col_labels = list(self._col_labels)

        # Restore original column names
        value_cols = []
        for column_id in self._block.value_columns:
            # If it is a replacement, then replace with column from right
            if column_id in sql_names:
                value_cols.append(get_column_right(v._value.get_name()).name(column_id))
            elif column_id:
                value_cols.append(get_column_left(column_id).name(column_id))

        # Assign a new column
        if not sql_names:
            # TODO(garrettwu): make sure sql name doesn't already exist.
            value_cols.append(get_column_right(v._value.get_name()).name(k))
            col_labels.append(k)

        block = joined_index._block
        block.replace_value_columns(value_cols)
        return DataFrame(joined_index, col_labels)

    def reset_index(self, *, drop: bool = False) -> DataFrame:
        """Reset the index of the DataFrame, and use the default one instead."""
        original_index_ids = self._block.index_columns
        original_index_label = self.index.name

        if len(original_index_ids) != 1:
            raise NotImplementedError("reset_index() doesn't yet support MultiIndex.")

        col_labels = list(self._col_labels)
        block = self._block.reset_index()

        if drop:
            # Even though the index might be part of the ordering, keep that
            # ordering expression as reset_index shouldn't change the row
            # order.
            block.expr = block.expr.drop_columns(original_index_ids)
        else:
            # TODO(swast): Support MultiIndex
            index_label = original_index_label
            if index_label is None:
                if "index" not in col_labels:
                    index_label = "index"
                else:
                    index_label = "level_0"

            if index_label in col_labels:
                raise ValueError(f"cannot insert {index_label}, already exists")

            col_labels = [index_label] + col_labels

        index = block.index
        index.name = None
        return DataFrame(index, col_labels)

    def set_index(self, key: str, *, drop: bool = True) -> DataFrame:
        """Set the DataFrame index using existing columns."""
        expr = self._block.expr
        prev_index_columns = self._block.index_columns
        index_expr = typing.cast(ibis_types.Column, expr.get_column(key))

        # TODO(swast): Don't override ordering once all DataFrames/Series have
        # an ordering.
        if not expr.ordering or (
            len(expr.ordering)
            and expr.ordering[0].get_name() == leanframe.core.ORDER_ID_COLUMN
        ):
            expr = expr.order_by([order.OrderingColumnReference(key)])

        expr = expr.drop_columns(prev_index_columns)

        col_labels = list(self._col_labels)
        index_columns = self._sql_names(key)
        if not drop:
            index_column_id = indexes.INDEX_COLUMN_ID.format(0)
            index_expr = index_expr.name(index_column_id)
            expr = expr.insert_column(0, index_expr)
            index_columns = [index_column_id]
        else:
            col_labels.remove(key)

        block = blocks.Block(expr, index_columns)
        index = block.index
        index.name = key
        return DataFrame(index, col_labels)

    def sort_index(self) -> DataFrame:
        """Sort the DataFrame by index labels."""
        index_columns = self._block.index_columns
        expr = self._block.expr.order_by(
            [order.OrderingColumnReference(column) for column in index_columns]
        )
        block = self._block.copy()
        block.expr = expr
        index = self._recreate_index(block)
        return DataFrame(index)

    def sort_values(
        self,
        by: str | typing.Sequence[str],
        *,
        ascending: bool | typing.Sequence[bool] = True,
        na_position: typing.Literal["first", "last"] = "last",
    ) -> DataFrame:
        """Sort dataframe ordering by value column(s)."""
        if na_position not in {"first", "last"}:
            raise ValueError("Param na_position must be one of 'first' or 'last'")

        sort_labels = (by,) if isinstance(by, str) else tuple(by)
        sort_column_ids = self._sql_names(sort_labels)

        len_by = len(sort_labels)
        if not isinstance(ascending, bool):
            if len(ascending) != len_by:
                raise ValueError("Length of 'ascending' must equal length of 'by'")
            sort_directions = ascending
        else:
            sort_directions = (ascending,) * len_by

        ordering = []
        for i in range(len(sort_labels)):
            column_id = sort_column_ids[i]
            direction = (
                order.OrderingDirection.ASC
                if sort_directions[i]
                else order.OrderingDirection.DESC
            )
            na_last = na_position == "last"
            ordering.append(
                order.OrderingColumnReference(
                    column_id, direction=direction, na_last=na_last
                )
            )

        block = self._block.copy()
        block.expr = block.expr.order_by(ordering)
        return DataFrame(
            block.index,
            columns=self._col_labels,
        )

    def dropna(self) -> DataFrame:
        """Remove rows with missing values."""
        predicates = [
            column.notnull()
            for column in self._block.expr.columns
            if column.get_name() in self._block.value_columns
        ]
        df = self._copy()
        for predicate in predicates:
            df._block.expr = df._block.expr.filter(predicate)
        return df

    def merge(
        self,
        right: DataFrame,
        how: Literal[
            "inner",
            "left",
            "outer",
            "right",
        ] = "inner",  # TODO(garrettwu): Currently can take inner, outer, left and right. To support cross joins
        # TODO(garrettwu): Support "on" list of columns and None. Currently a single column must be provided
        on: Optional[str] = None,
        suffixes: tuple[str, str] = ("_x", "_y"),
    ) -> DataFrame:
        """Merge DataFrame objects with a database-style join."""
        if not on:
            raise ValueError("Must specify a column to join on.")

        left = self
        left_on_sql = self._sql_names(on)
        # 0 elements alreasy throws an exception
        if len(left_on_sql) > 1:
            raise ValueError(f"The column label {on} is not unique.")
        left_on_sql = left_on_sql[0]

        right_on_sql = right._sql_names(on)
        if len(right_on_sql) > 1:
            raise ValueError(f"The column label {on} is not unique.")
        right_on_sql = right_on_sql[0]

        (
            joined_expr,
            join_key_id,
            (get_column_left, get_column_right),
        ) = joins.join_by_column(
            left._block.expr,
            left_on_sql,
            right._block.expr,
            right_on_sql,
            how=how,
        )
        # TODO(swast): Add suffixes to the column labels instead of reusing the
        # column IDs as the new labels.
        # Drop the index column(s) to be consistent with pandas.
        joined_expr = joined_expr.projection(
            [
                (
                    joined_expr.get_column(join_key_id)
                    if col_id == left_on_sql
                    else get_column_left(col_id)
                )
                for col_id in left._block.value_columns
            ]
            + [
                get_column_right(col_id)
                for col_id in right._block.value_columns
                if col_id != right_on_sql
            ]
        )

        block = blocks.Block(joined_expr)
        # TODO(swast): Need to reset to a sequential index and materialize to
        # be fully consistent with pandas.
        index = indexes.ImplicitJoiner(block)
        col_labels = self._get_merged_col_labels(right, on=on, suffixes=suffixes)
        return DataFrame(index, columns=col_labels)

    def _get_merged_col_labels(
        self, right: DataFrame, on: str, suffixes: tuple[str, str] = ("_x", "_y")
    ) -> List[str]:
        left_col_labels = [
            (
                ("{name}" + suffixes[0]).format(name=col_label)
                if col_label in right._col_labels and col_label != on
                else col_label
            )
            for col_label in self._col_labels
        ]
        right_col_labels = [
            (
                ("{name}" + suffixes[1]).format(name=col_label)
                if col_label in self._col_labels and col_label != on
                else col_label
            )
            for col_label in right._col_labels
            if col_label != on
        ]
        return left_col_labels + right_col_labels

    def join(self, other: DataFrame, how: str) -> DataFrame:
        """Join columns of another dataframe"""

        if not self.columns.intersection(other.columns).empty:
            raise NotImplementedError("Deduping column names is not implemented")

        left = self
        right = other
        combined_index, (get_column_left, get_column_right) = left.index.join(
            right.index, how=how
        )

        block = combined_index._block

        index_columns = []
        if isinstance(combined_index, indexes.Index):
            index_columns = [
                # TODO(swast): Support MultiIndex.
                combined_index._expr.get_any_column(combined_index._index_column)
            ]

        expr_bldr = block.expr.builder()
        # TODO(swast): Move this logic to BigFramesExpr once there's a list of
        # which columns are value columns and which are index columns there.
        expr_bldr.columns = (
            index_columns
            + [
                # TODO(swast): Support suffix if there are duplicates.
                get_column_left(col_id).name(col_id)
                for col_id in left._sql_names(left.columns.to_list())
            ]
            + [
                # TODO(swast): Support suffix if there are duplicates.
                get_column_right(col_id).name(col_id)
                for col_id in right._sql_names(right.columns.to_list())
            ]
        )
        block.expr = expr_bldr.build()
        return DataFrame(combined_index, self._col_labels + other._col_labels)

    def groupby(
        self,
        by: typing.Union[str, typing.Sequence[str]],
        *,
        dropna: bool = True,
        as_index=True,
    ) -> groupby.DataFrameGroupBy:
        """Group the dataframe by a given list of column labels.

        Arguments:
            by: a column label or list of column labels to group on
            dropna: NA-valued grouping keys will be dropped from result if True
            as_index: grouping keys will be used as index if True
        Returns:
            DataFrameGroupBy
        """
        if as_index and not isinstance(by, str):
            raise ValueError(
                "Set as_index=False if grouping by list of values. Mutli-index not yet supported"
            )
        labels = self._col_labels
        col_ids = self._block.value_columns
        label_id_pairs = {col_id: label for col_id, label in zip(col_ids, labels)}
        by_col_ids = self._sql_names(by)
        return groupby.DataFrameGroupBy(
            self._block.copy(),
            label_id_pairs,
            by_col_ids,
            dropna=dropna,
            as_index=as_index,
        )

    def abs(self) -> DataFrame:
        """Calculates the absolute value of elements in the dataframe."""
        return self._apply_to_rows(ops.abs_op)

    def isnull(self) -> DataFrame:
        """Maps NA values to True and non-NA values to False."""
        return self._apply_to_rows(ops.isnull_op)

    isna = isnull

    def notnull(self) -> DataFrame:
        """Maps NA values to False and non-NA values to True."""
        return self._apply_to_rows(ops.notnull_op)

    notna = notnull

    def cumsum(self):
        """Applies cumulative sum over an axis. All values must be numeric."""
        is_numeric_types = [
            (dtype in leanframe.dtypes.NUMERIC_BIGFRAMES_TYPES)
            for _, dtype in self.dtypes.items()
        ]
        if not all(is_numeric_types):
            raise ValueError("All values must be numeric to apply cumsum.")
        return self._apply_window_op(
            agg_ops.sum_op,
            leanframe.core.WindowSpec(following=0),
        )

    def cumprod(self) -> DataFrame:
        """Applies cumulative product over an axis. All values must be numeric."""
        is_numeric_types = [
            (dtype in leanframe.dtypes.NUMERIC_BIGFRAMES_TYPES)
            for _, dtype in self.dtypes.items()
        ]
        if not all(is_numeric_types):
            raise ValueError("All values must be numeric to apply cumsum.")
        return self._apply_window_op(
            agg_ops.product_op,
            leanframe.core.WindowSpec(following=0),
        )

    def cummin(self) -> DataFrame:
        """Applies cumulative min over an axis."""
        return self._apply_window_op(
            agg_ops.min_op,
            leanframe.core.WindowSpec(following=0),
        )

    def cummax(self) -> DataFrame:
        """Applies cumulative max over an axis."""
        return self._apply_window_op(
            agg_ops.max_op,
            leanframe.core.WindowSpec(following=0),
        )

    def shift(self, periods=1) -> DataFrame:
        """Shift index by desired number of periods."""
        window = leanframe.core.WindowSpec(
            preceding=periods if periods > 0 else None,
            following=-periods if periods < 0 else None,
        )
        return self._apply_window_op(agg_ops.ShiftOp(periods), window)

    def _apply_window_op(
        self,
        op: agg_ops.WindowOp,
        window_spec: leanframe.core.WindowSpec,
    ):
        block = self._block.copy()
        for col_id in self._block.value_columns[:-1]:
            block.apply_window_op(
                col_id, op, window_spec=window_spec, skip_reproject_unsafe=True
            )
        # Reproject after applying final independent window operation.
        block.apply_window_op(
            self._block.value_columns[-1], op, window_spec=window_spec
        )
        return DataFrame(block.index, self._col_labels)

    def to_pandas(self) -> pd.DataFrame:
        """Writes DataFrame to Pandas DataFrame."""
        # TODO(chelsealin): Support block parameters.
        # TODO(chelsealin): Add to_pandas_batches() API.
        return self.compute()

    def to_csv(self, paths: str, *, index: bool = True) -> None:
        """Writes DataFrame to comma-separated values (csv) file(s) on GCS.

        Args:
            paths: a destination URIs of GCS files(s) to store the extracted dataframe in format of
                ``gs://<bucket_name>/<object_name_or_glob>``.
                If the data size is more than 1GB, you must use a wildcard to export the data into
                multiple files and the size of the files varies.

            index: whether write row names (index) or not.

        Returns:
            None.
        """
        # TODO(swast): Can we support partition columns argument?
        # TODO(chelsealin): Support local file paths.
        # TODO(swast): Some warning that wildcard is recommended for large
        # query results? See:
        # https://cloud.google.com/bigquery/docs/exporting-data#limit_the_exported_file_size
        if not paths.startswith("gs://"):
            raise NotImplementedError(
                "Only Google Cloud Storage (gs://...) paths are supported."
            )

        source_table = self._execute_query(index=index)
        job_config = bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.CSV
        )
        extract_job = self._block.expr._session.bqclient.extract_table(
            source_table, destination_uris=[paths], job_config=job_config
        )
        extract_job.result()  # Wait for extract job to finish

    def to_gbq(
        self,
        destination_table: str,
        *,
        if_exists: Optional[Literal["fail", "replace", "append"]] = "fail",
        index: bool = True,
    ) -> None:
        """Writes the BigFrames DataFrame as a BigQuery table.

        Args:
            destination_table: name of table to be written, in the form
                `dataset.tablename` or `project.dataset.tablename`.

            if_exists: behavior when the destination table exists. Value can be one of:
                - `fail`: raise google.api_core.exceptions.Conflict.
                - `replace`: If table exists, drop it, recreate it, and insert data.
                - `append`: If table exists, insert data. Create if it does not exist.

            index: whether write row names (index) or not.

        Returns:
            None.
        """

        if "." not in destination_table:
            raise ValueError(
                "Invalid Table Name. Should be of the form 'datasetId.tableId' or "
                "'projectId.datasetId.tableId'"
            )

        dispositions = {
            "fail": bigquery.WriteDisposition.WRITE_EMPTY,
            "replace": bigquery.WriteDisposition.WRITE_TRUNCATE,
            "append": bigquery.WriteDisposition.WRITE_APPEND,
        }
        if if_exists not in dispositions:
            raise ValueError("'{0}' is not valid for if_exists".format(if_exists))

        job_config = bigquery.QueryJobConfig(
            write_disposition=dispositions[if_exists],
            destination=bigquery.table.TableReference.from_string(
                destination_table,
                default_project=self._block.expr._session.bqclient.project,
            ),
        )

        self._execute_query(index=index, job_config=job_config)

    def to_parquet(self, paths: str, *, index: bool = True) -> None:
        """Writes DataFrame to parquet file(s) on GCS.

        Args:
            paths: a destination URIs of GCS files(s) to store the extracted dataframe in format of
                ``gs://<bucket_name>/<object_name_or_glob>``.
                If the data size is more than 1GB, you must use a wildcard to export the data into
                multiple files and the size of the files varies.

            index: whether write row names (index) or not.

        Returns:
            None.
        """
        # TODO(swast): Can we support partition columns argument?
        # TODO(chelsealin): Support local file paths.
        # TODO(swast): Some warning that wildcard is recommended for large
        # query results? See:
        # https://cloud.google.com/bigquery/docs/exporting-data#limit_the_exported_file_size
        if not paths.startswith("gs://"):
            raise NotImplementedError(
                "Only Google Cloud Storage (gs://...) paths are supported."
            )

        source_table = self._execute_query(index=index)
        job_config = bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.PARQUET
        )
        extract_job = self._block.expr._session.bqclient.extract_table(
            source_table, destination_uris=[paths], job_config=job_config
        )
        extract_job.result()  # Wait for extract job to finish

    def _apply_to_rows(self, operation: ops.UnaryOp):
        columns = self._block.get_value_col_exprs()
        new_columns = [
            operation._as_ibis(column).name(column.get_name()) for column in columns
        ]
        return self._copy((new_columns, self._col_labels))

    def _execute_query(
        self, index: bool, job_config: Optional[bigquery.job.QueryJobConfig] = None
    ):
        """Executes a query job presenting this dataframe and returns the destination
        table."""
        expr = self._block.expr
        columns = self.columns.tolist()
        # This code drops unnamed indexes to keep consistent with the behavior of
        # most pandas write APIs. The exception is `pandas.to_csv`, which keeps
        # unnamed indexes as `Unnamed: 0`.
        # TODO(chelsealin): check if works for multiple indexes.
        if index and self.index.name is not None:
            columns.append(self.index.name)
        # TODO(chelsealin): onboard IO to standardize names to reflect the label
        # after b/282205091. Add tests when the column name (label) is not equal to
        # the column ID.
        # TODO(chelsealin): normalize the file formats if we needs, such as arbitrary
        # unicode for column labels.
        value_columns = (expr.get_column(column_name) for column_name in columns)
        expr = expr.projection(value_columns)
        query_job: bigquery.QueryJob = expr.start_query(job_config)
        query_job.result()  # Wait for query to finish.
        query_job.reload()  # Download latest job metadata.
        return query_job.destination
