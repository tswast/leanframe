# Copyright 2023 Google LLC
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

from dataclasses import dataclass
import functools
import math
import typing
from typing import Collection, Dict, Iterable, Optional, Sequence

from google.cloud import bigquery
import ibis
import ibis.expr.types as ibis_types

import leanframe.aggregations as agg_ops
from leanframe.core.ordering import (
    ExpressionOrdering,
    OrderingColumnReference,
    stringify_order_id,
)
import leanframe.guid
import leanframe.operations as ops

if typing.TYPE_CHECKING:
    from leanframe.session import Session

ORDER_ID_COLUMN = "leanframe_ordering_id"
PREDICATE_COLUMN = "leanframe_predicate"


@dataclass(frozen=True)
class WindowSpec:
    """
    Specifies a window over which aggregate and analytic function may be applied.
    grouping_keys: set of column ids to group on
    preceding: Number of preceding rows in the window
    following: Number of preceding rows in the window
    ordering: List of columns ids and ordering direction to override base ordering
    """

    grouping_keys: typing.Sequence[str] = tuple()
    ordering: typing.Sequence[OrderingColumnReference] = tuple()
    preceding: typing.Optional[int] = None
    following: typing.Optional[int] = None
    min_periods: int = 0


# TODO(swast): We might want to move this to it's own sub-module.
class BigFramesExpr:
    """Immutable BigFrames expression tree.
    Note: Usage of this class is considered to be private and subject to change
    at any time.
    This class is a wrapper around Ibis expressions. Its purpose is to defer
    Ibis projection operations to keep generated SQL small and correct when
    mixing and matching columns from different versions of a DataFrame.
    Args:
        session:
            A BigFrames session to allow more flexibility in running
            queries.
        table: An Ibis table expression.
        columns: Ibis value expressions that can be projected as columns.
        meta_columns: Ibis value expressions to store ordering.
        ordering: An ordering property of the data frame.
        predicates: A list of filters on the data frame.
    """

    def __init__(
        self,
        session: Session,
        table: ibis_types.Table,
        columns: Optional[Sequence[ibis_types.Value]] = None,
        meta_columns: Optional[Sequence[ibis_types.Value]] = None,
        ordering: Optional[ExpressionOrdering] = None,
        predicates: Optional[Collection[ibis_types.BooleanValue]] = None,
    ):
        self._session = session
        self._table = table
        self._predicates = tuple(predicates) if predicates is not None else ()
        # TODO: Validate ordering
        self._ordering = ordering or ExpressionOrdering()
        # Allow creating a DataFrame directly from an Ibis table expression.
        if columns is None:
            self._columns = tuple(
                table[key]
                for key in table.columns
                if ordering is None or key != ordering.ordering_id
            )
        else:
            # TODO(swast): Validate that each column references the same table (or
            # no table for literal values).
            self._columns = tuple(columns)

        # Meta columns store ordering, or other data that doesn't correspond to dataframe columns
        self._meta_columns = tuple(meta_columns) if meta_columns is not None else ()

        # To allow for more efficient lookup by column name, create a
        # dictionary mapping names to column values.
        self._column_names = {column.get_name(): column for column in self._columns}
        self._meta_column_names = {
            column.get_name(): column for column in self._meta_columns
        }

    @property
    def table(self) -> ibis_types.Table:
        return self._table

    @property
    def predicates(self) -> typing.Tuple[ibis_types.BooleanValue, ...]:
        return self._predicates

    @property
    def reduced_predicate(self) -> typing.Optional[ibis_types.BooleanValue]:
        """Returns the frame's predicates as an equivalent boolean value, useful where a single predicate value is preferred."""
        return (
            _reduce_predicate_list(self._predicates).name(PREDICATE_COLUMN)
            if self._predicates
            else None
        )

    @property
    def columns(self) -> typing.Tuple[ibis_types.Value, ...]:
        return self._columns

    @property
    def column_names(self) -> Dict[str, ibis_types.Value]:
        return self._column_names

    @property
    def meta_columns(self) -> typing.Tuple[ibis_types.Value, ...]:
        return self._meta_columns

    @property
    def ordering(self) -> Sequence[ibis_types.Value]:
        """Returns a sequence of ibis values which can be directly used to order a table expression. Has direction modifiers applied."""
        if not self._ordering:
            return []
        else:
            # TODO(swast): When we assign literals / scalars, we might not
            # have a true Column. Do we need to check this before trying to
            # sort by such a column?
            return _convert_ordering_to_table_values(
                {**self._column_names, **self._meta_column_names},
                self._ordering.all_ordering_columns,
            )

    def builder(self) -> BigFramesExprBuilder:
        """Creates a mutable builder for expressions."""
        # Since BigFramesExpr is intended to be immutable (immutability offers
        # potential opportunities for caching, though we might need to introduce
        # more node types for that to be useful), we create a builder class.
        return BigFramesExprBuilder(
            self._session,
            self._table,
            self._columns,
            self._meta_columns,
            ordering=self._ordering,
            predicates=self._predicates,
        )

    def insert_column(self, index: int, column: ibis_types.Value) -> BigFramesExpr:
        expr = self.builder()
        expr.columns.insert(index, column)
        return expr.build()

    def drop_columns(self, columns: Iterable[str]) -> BigFramesExpr:
        # Must generate offsets if we are dropping a column that ordering depends on
        expr = self
        for ordering_column in set(columns).intersection(
            [col.column_id for col in self._ordering.ordering_value_columns]
        ):
            expr = self._hide_column(ordering_column)

        expr_builder = expr.builder()
        remain_cols = [
            column for column in expr.columns if column.get_name() not in columns
        ]
        expr_builder.columns = remain_cols
        return expr_builder.build()

    def get_column(self, key: str) -> ibis_types.Value:
        """Gets the Ibis expression for a given column."""
        if key not in self._column_names.keys():
            raise ValueError(
                "Column name {} not in set of values: {}".format(
                    key, self._column_names.keys()
                )
            )
        return typing.cast(ibis_types.Value, self._column_names[key])

    def get_any_column(self, key: str) -> ibis_types.Value:
        """Gets the Ibis expression for a given column. Will also get hidden meta columns."""
        all_columns = {**self._column_names, **self._meta_column_names}
        if key not in all_columns.keys():
            raise ValueError(
                "Column name {} not in set of values: {}".format(
                    key, all_columns.keys()
                )
            )
        return typing.cast(ibis_types.Value, all_columns[key])

    def _get_meta_column(self, key: str) -> ibis_types.Value:
        """Gets the Ibis expression for a given metadata column."""
        if key not in self._meta_column_names.keys():
            raise ValueError(
                "Column name {} not in set of values: {}".format(
                    key, self._meta_column_names.keys()
                )
            )
        return self._meta_column_names[key]

    def apply_limit(self, max_results: int) -> BigFramesExpr:
        table = self.to_ibis_expr().limit(max_results)
        # Since we make a new table expression, the old column references now
        # point to the wrong table. Use the BigFramesExpr constructor to make
        # sure we have the correct references.
        return BigFramesExpr(self._session, table)

    def filter(self, predicate: ibis_types.BooleanValue) -> BigFramesExpr:
        """Filter the table on a given expression, the predicate must be a boolean series aligned with the table expression."""
        expr = self.builder()
        if expr.ordering:
            expr.ordering = expr.ordering.with_is_sequential(False)
        expr.predicates = [*self._predicates, predicate]
        return expr.build()

    def order_by(self, by: Sequence[OrderingColumnReference]) -> BigFramesExpr:
        expr_builder = self.builder()
        expr_builder.ordering = self._ordering.with_ordering_columns(by)
        return expr_builder.build()

    @property
    def offsets(self):
        if not self._ordering.is_sequential:
            raise ValueError(
                "Expression does not have offsets. Generate them first using project_offsets."
            )
        return self._get_meta_column(self._ordering.ordering_id)

    def project_offsets(self) -> BigFramesExpr:
        """Create a new expression that contains offsets. Should only be executed when offsets are needed for an operations. Has no effect on expression semantics."""
        if self._ordering.is_sequential:
            return self
        # TODO(tbergeron): Enforce total ordering
        table = self.to_ibis_expr(
            ordering_mode="offset_col", order_col_name=ORDER_ID_COLUMN
        )
        columns = [table[column_name] for column_name in self._column_names]
        ordering = ExpressionOrdering(
            ordering_id_column=OrderingColumnReference(ORDER_ID_COLUMN),
            is_sequential=True,
        )
        return BigFramesExpr(
            self._session,
            table,
            columns=columns,
            meta_columns=[table[ORDER_ID_COLUMN]],
            ordering=ordering,
        )

    def _hide_column(self, column_id) -> BigFramesExpr:
        """Pushes columns to metadata columns list. Used to hide ordering columns that have been dropped or destructively mutated."""
        expr_builder = self.builder()
        # Need to rename column as caller might be creating a new row with the same name but different values.
        # Can avoid this if don't allow callers to determine ids and instead generate unique ones in this class.
        new_name = leanframe.guid.generate_guid(prefix="leanframe_meta_")
        expr_builder.meta_columns = [
            *self._meta_columns,
            self.get_column(column_id).name(new_name),
        ]

        ordering_columns = [
            col if col.column_id != column_id else col.with_name(new_name)
            for col in self._ordering.ordering_value_columns
        ]

        expr_builder.ordering = self._ordering.with_ordering_columns(ordering_columns)
        return expr_builder.build()

    def promote_offsets(self, value_col_id: str) -> BigFramesExpr:
        """
        Convenience function to promote copy of column offsets to a value column. Can be used to reset index.

        Args:
            value_col_id: The id that will be used for the resulting column id. Should not match any existing column ids.
            is_reverse: If true, will instead generate a value column using offsets in reverse order.
        """
        # Special case: offsets already exist
        # TODO(tbergeron): Create version that generates reverse offsets (for negative indexing)
        ordering = self._ordering
        if (not ordering.is_sequential) or (not ordering.ordering_id):
            return self.project_offsets().promote_offsets(value_col_id)
        expr_builder = self.builder()
        expr_builder.columns = [
            self._get_meta_column(ordering.ordering_id).name(value_col_id),
            *self.columns,
        ]
        return expr_builder.build()

    def projection(self, columns: Iterable[ibis_types.Value]) -> BigFramesExpr:
        """Creates a new expression based on this expression with new columns."""
        # TODO(swast): We might want to do validation here that columns derive
        # from the same table expression instead of (in addition to?) at
        # construction time.

        expr = self
        for ordering_column in set(self.column_names.keys()).intersection(
            [col_ref.column_id for col_ref in self._ordering.ordering_value_columns]
        ):
            # Need to hide ordering columns that are being dropped. Alternatively, could project offsets
            expr = expr._hide_column(ordering_column)
        builder = expr.builder()
        builder.columns = list(columns)
        new_expr = builder.build()
        return new_expr

    def shape(self) -> typing.Tuple[int, int]:
        """Returns dimensions as (length, width) tuple."""
        width = len(self.columns)
        length_query = self._session.bqclient.query(
            self.to_ibis_expr(ordering_mode="unordered").count().compile()
        )
        length = next(length_query.result())[0]
        return (length, width)

    def concat(self, other: typing.Sequence[BigFramesExpr]) -> BigFramesExpr:
        """Append together multiple BigFramesExpressions."""
        if len(other) == 0:
            return self
        tables = []
        prefix_base = 10
        prefix_size = math.ceil(math.log(len(other) + 1, prefix_base))
        # Must normalize all ids to the same encoding size
        max_encoding_size = max(
            self._ordering.ordering_encoding_size,
            *[expression._ordering.ordering_encoding_size for expression in other],
        )
        for i, expr in enumerate([self, *other]):
            ordering_prefix = str(i).zfill(prefix_size)
            table = expr.to_ibis_expr(
                ordering_mode="ordered_col", order_col_name=ORDER_ID_COLUMN
            )
            # Rename the value columns based on horizontal offset before applying union.
            table = table.select(
                [
                    col
                    if col != ORDER_ID_COLUMN
                    else (
                        ordering_prefix
                        + stringify_order_id(table[ORDER_ID_COLUMN], max_encoding_size)
                    ).name(ORDER_ID_COLUMN)
                    for col in table.columns
                ]
            )
            tables.append(table)
        combined_table = ibis.union(*tables)
        ordering = ExpressionOrdering(
            ordering_id_column=OrderingColumnReference(ORDER_ID_COLUMN),
            ordering_encoding_size=prefix_size + max_encoding_size,
        )
        return BigFramesExpr(
            self._session,
            combined_table,
            columns=[
                combined_table[col]
                for col in combined_table.columns
                if col != ORDER_ID_COLUMN
            ],
            meta_columns=[combined_table[ORDER_ID_COLUMN]],
            ordering=ordering,
        )

    def project_unary_op(
        self, column_name: str, op: ops.UnaryOp, output_name=None
    ) -> BigFramesExpr:
        """Creates a new expression based on this expression with unary operation applied to one column."""
        value = op._as_ibis(self.get_column(column_name)).name(
            output_name or column_name
        )
        return self._set_or_replace_by_id(output_name or column_name, value)

    def project_binary_op(
        self,
        left_column_id: str,
        right_column_id: str,
        op: ops.BinaryOp,
        output_column_id: str,
    ) -> BigFramesExpr:
        """Creates a new expression based on this expression with binary operation applied to two columns."""
        value = op(
            self.get_column(left_column_id), self.get_column(right_column_id)
        ).name(output_column_id)
        return self._set_or_replace_by_id(output_column_id, value)

    def aggregate(
        self,
        by_column_ids: typing.Sequence[str],
        aggregations: typing.Sequence[typing.Tuple[str, agg_ops.AggregateOp, str]],
        dropna: bool = True,
    ) -> BigFramesExpr:
        """
        Apply aggregations to the expression.
        Arguments:
            by_column_id: column id of the aggregation key, this is preserved through the transform
            aggregations: input_column_id, operation, output_column_id tuples
            dropna: whether null keys should be dropped
        """
        table = self.to_ibis_expr()
        stats = [
            agg_op._as_ibis(table[col_in]).name(col_out)
            for col_in, agg_op, col_out in aggregations
        ]
        result = table.group_by(by_column_ids).aggregate(stats)
        # Must have deterministic ordering, so order by the unique "by" column
        ordering = ExpressionOrdering(
            [
                OrderingColumnReference(column_id=column_id)
                for column_id in by_column_ids
            ]
        )
        expr = BigFramesExpr(self._session, result, ordering=ordering)
        if dropna:
            for column_id in by_column_ids:
                expr = expr.filter(ops.notnull_op._as_ibis(expr.get_column(column_id)))
        # Can maybe remove this as Ordering id is redundant as by_column is unique after aggregation
        return expr.project_offsets()

    def project_window_op(
        self,
        column_name: str,
        op: agg_ops.WindowOp,
        window_spec: WindowSpec,
        output_name=None,
        *,
        skip_reproject_unsafe: bool = False,
    ) -> BigFramesExpr:
        """
        Creates a new expression based on this expression with unary operation applied to one column.
        column_name: the id of the input column present in the expression
        op: the windowable operator to apply to the input column
        window_spec: a specification of the window over which to apply the operator
        output_name: the id to assign to the output of the operator, by default will replace input col if distinct output id not provided
        skip_reproject_unsafe: skips the reprojection step, can be used when performing many non-dependent window operations, user responsible for not nesting window expressions, or using outputs as join, filter or aggregation keys before a reprojection
        """
        column = typing.cast(ibis_types.Column, self.get_column(column_name))
        window = self._ibis_window_from_spec(window_spec)

        window_op = op._as_ibis(column, window)

        clauses = []
        if op.skips_nulls:
            clauses.append((column.isnull(), ibis.NA))
        if window_spec.min_periods:
            clauses.append(
                (
                    agg_ops.count_op._as_ibis(column, window)
                    < ibis_types.literal(window_spec.min_periods),
                    ibis.NA,
                )
            )

        if clauses:
            case_statement = ibis.case()
            for clause in clauses:
                case_statement = case_statement.when(clause[0], clause[1])
            case_statement = case_statement.else_(window_op).end()
            window_op = case_statement

        result = self._set_or_replace_by_id(output_name or column_name, window_op)
        # TODO(tbergeron): Automatically track analytic expression usage and defer reprojection until required for valid query generation.
        return result._reproject_to_table() if not skip_reproject_unsafe else result

    def to_ibis_expr(
        self,
        ordering_mode: str = "order_by",
        order_col_name=ORDER_ID_COLUMN,
    ):
        """
        Creates an Ibis table expression representing the DataFrame.

        BigFrames expression are sorted, so three options are avaiable to reflect this in the ibis expression.
        The default is that the expression will be ordered by an order_by clause.
        "order_by": The output table will not have an ordering column, however there will be an order_by clause applied to the ouput.
        "offset_col": Zero-based offsets are generated as a column, this will not sort the rows however.
        "ordered_col": An ordered column is provided in output table, without guarantee that the values are sequential
        "expose_metadata": All columns projected in table expression, including hidden columns. Output is not otherwise ordered
        "unordered": No ordering information will be provided in output. Only value columns are projected.
        For offset or ordered column, order_col_name can be used to assign the output label for the ordering column.
        If none is specified, the default column name will be 'bigrames_ordering_id'

        Args:
            with_offsets: Output will include 0-based offsets as a column if set to True
            ordering_mode: One of "order_by", "ordered_col", or "offset_col"
        Returns:
            An ibis expression representing the data help by the BigFramesExpression.
        """
        assert ordering_mode in (
            "order_by",
            "ordered_col",
            "offset_col",
            "expose_metadata",
            "unordered",
        )

        table = self._table
        columns = list(self._columns)
        hidden_ordering_columns = [
            col.column_id
            for col in self._ordering.all_ordering_columns
            if col.column_id not in self._column_names.keys()
        ]

        if self.reduced_predicate is not None:
            columns.append(self.reduced_predicate)
        if ordering_mode in ("offset_col", "ordered_col"):
            # Generate offsets if current ordering id semantics are not sufficiently strict
            if (ordering_mode == "offset_col" and not self._ordering.is_sequential) or (
                ordering_mode == "ordered_col" and not self._ordering.order_id_defined
            ):
                window = ibis.window(order_by=self.ordering)
                if self._predicates:
                    window = window.group_by(self.reduced_predicate)
                columns.append(ibis.row_number().name(order_col_name).over(window))
            elif self._ordering.ordering_id:
                columns.append(
                    self._get_meta_column(self._ordering.ordering_id).name(
                        order_col_name
                    )
                )
            else:
                # Should not be possible.
                raise ValueError(
                    "Expression does not have ordering id and none was generated."
                )
        elif ordering_mode == "order_by":
            columns.extend(
                [self._get_meta_column(name) for name in hidden_ordering_columns]
            )
        elif ordering_mode == "expose_metadata":
            columns.extend(self._meta_columns)

        # Special case for empty tables, since we can't create an empty
        # projection.
        if not columns:
            return ibis.memtable([])
        table = table.select(columns)
        if self.reduced_predicate is not None:
            table = table.filter(table[PREDICATE_COLUMN])
            # Drop predicate as it is will be all TRUE after filtering
            table = table.drop(PREDICATE_COLUMN)
        if ordering_mode == "order_by":
            # Some ordering columns are value columns, while other are used purely for ordering.
            # We drop the non-value columns after the ordering
            table = table.order_by(
                _convert_ordering_to_table_values(
                    {col: table[col] for col in table.columns},
                    self._ordering.all_ordering_columns,
                )  # type: ignore
            )
            if not (ordering_mode == "expose_metadata"):
                table = table.drop(*hidden_ordering_columns)
        return table

    def start_query(
        self, job_config: Optional[bigquery.job.QueryJobConfig] = None
    ) -> bigquery.QueryJob:
        """Execute a query and return metadata about the results."""
        # TODO(swast): Cache the job ID so we can look it up again if they ask
        # for the results? We'd need a way to invalidate the cache if DataFrame
        # becomes mutable, though. Or move this method to the immutable
        # expression class.
        # TODO(swast): We might want to move this method to Session and/or
        # provide our own minimal metadata class. Tight coupling to the
        # BigQuery client library isn't ideal, especially if we want to support
        # a LocalSession for unit testing.
        # TODO(swast): Add a timeout here? If the query is taking a long time,
        # maybe we just print the job metadata that we have so far?
        table = self.to_ibis_expr()
        sql = table.compile()
        if job_config is not None:
            return self._session.bqclient.query(sql, job_config=job_config)
        else:
            return self._session.bqclient.query(sql)

    def _reproject_to_table(self):
        """
        Internal operators that projects the internal representation into a
        new ibis table expression where each value column is a direct
        reference to a column in that table expression. Needed after
        some operations such as window operations that cannot be used
        recursively in projections.
        """
        table = self.to_ibis_expr(
            ordering_mode="expose_metadata", order_col_name=self._ordering.ordering_id
        )
        columns = [table[column_name] for column_name in self._column_names]
        meta_columns = [table[column_name] for column_name in self._meta_column_names]
        return BigFramesExpr(
            self._session,
            table,
            columns=columns,
            meta_columns=meta_columns,
            ordering=self._ordering,
        )

    def _ibis_window_from_spec(self, window_spec: WindowSpec):
        group_by: typing.List[ibis_types.Value] = (
            [
                typing.cast(ibis_types.Column, self.get_column(column))
                for column in window_spec.grouping_keys
            ]
            if window_spec.grouping_keys
            else []
        )
        if self.reduced_predicate is not None:
            group_by.append(self.reduced_predicate)
        if window_spec.ordering:
            order_overrides = _convert_ordering_to_table_values(
                {**self._column_names, **self._meta_column_names}, window_spec.ordering
            )
            order_by = tuple([*order_overrides, *self.ordering])
        elif (window_spec.following is not None) or (window_spec.preceding is not None):
            # If window spec has following or preceding bounds, we need to apply an unambiguous ordering.
            order_by = tuple(self.ordering)
        else:
            # Unbound grouping window. Suitable for aggregations but not for analytic function application.
            order_by = None
        return ibis.window(
            preceding=window_spec.preceding,
            following=window_spec.following,
            order_by=order_by,
            group_by=group_by,
        )

    # TODO(b/282041134) Remove deprecate_rename_column once label/id separation in dataframe
    def deprecated_rename_column(self, old_id, new_id) -> BigFramesExpr:
        """
        Don't use this, temporary measure until dataframe supports sqlid!=dataframe col id.
        In future, caller shouldn't need to control internal column id strings.
        """
        if new_id == old_id:
            return self
        return self._set_or_replace_by_id(new_id, self.get_column(old_id)).drop_columns(
            [old_id]
        )

    def _set_or_replace_by_id(self, id: str, value: ibis_types.Value):
        expr = self
        if id in self.column_names:
            expr = expr.drop_columns([id])
        builder = expr.builder()
        builder.columns = [*expr.columns, value.name(id)]
        return builder.build()


class BigFramesExprBuilder:
    """Mutable expression class.
    Use BigFramesExpr.builder() to create from a BigFramesExpr object.
    """

    def __init__(
        self,
        session: Session,
        table: ibis_types.Table,
        columns: Collection[ibis_types.Value] = (),
        meta_columns: Collection[ibis_types.Value] = (),
        ordering: Optional[ExpressionOrdering] = None,
        predicates: Optional[Collection[ibis_types.BooleanValue]] = None,
    ):
        self.session = session
        self.table = table
        self.columns = list(columns)
        self.meta_columns = list(meta_columns)
        self.ordering = ordering
        self.predicates = list(predicates) if predicates is not None else None

    def build(self) -> BigFramesExpr:
        return BigFramesExpr(
            session=self.session,
            table=self.table,
            columns=self.columns,
            meta_columns=self.meta_columns,
            ordering=self.ordering,
            predicates=self.predicates,
        )


def _reduce_predicate_list(
    predicate_list: typing.Collection[ibis_types.BooleanValue],
) -> ibis_types.BooleanValue:
    """Converts a list of predicates BooleanValues into a single BooleanValue."""
    if len(predicate_list) == 0:
        raise ValueError("Cannot reduce empty list of predicates")
    if len(predicate_list) == 1:
        (item,) = predicate_list
        return item
    return functools.reduce(lambda acc, pred: acc.__and__(pred), predicate_list)


def _convert_ordering_to_table_values(
    value_lookup: typing.Mapping[str, ibis_types.Value],
    ordering_columns: typing.Sequence[OrderingColumnReference],
) -> typing.Sequence[ibis_types.Value]:
    column_refs = ordering_columns
    ordering_values = []
    for ordering_col in column_refs:
        column = typing.cast(ibis_types.Column, value_lookup[ordering_col.column_id])
        ordering_value = (
            ibis.asc(column)
            if ordering_col.direction.is_ascending
            else ibis.desc(column)
        )
        # Bigquery SQL considers NULLS to be "smallest" values, but we need to override in these cases.
        if (not ordering_col.na_last) and (not ordering_col.direction.is_ascending):
            # Force nulls to be first
            is_null_val = typing.cast(ibis_types.Column, column.isnull())
            ordering_values.append(ibis.desc(is_null_val))
        elif (ordering_col.na_last) and (ordering_col.direction.is_ascending):
            # Force nulls to be last
            is_null_val = typing.cast(ibis_types.Column, column.isnull())
            ordering_values.append(ibis.asc(is_null_val))
        ordering_values.append(ordering_value)
    return ordering_values
