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

import typing

import leanframe.aggregations as agg_ops
import leanframe.core.blocks as blocks
import leanframe.dataframe as df
import leanframe.dtypes


class DataFrameGroupBy:
    """Represents a deferred dataframe with a grouping expression."""

    def __init__(
        self,
        block: blocks.Block,
        col_id_labels: typing.Mapping[str, str],
        by_col_ids: typing.Sequence[str],
        *,
        dropna: bool = True,
        as_index: bool = True,
    ):
        if len(by_col_ids) > 1 and as_index:
            raise ValueError(
                "Set as_index=False if grouping by multiple values. Mutli-index not yet supported"
            )
        # TODO(tbergeron): Support more group-by expression types
        self._block = block
        self._col_id_labels = col_id_labels
        self._by_col_ids = by_col_ids
        self._dropna = dropna  # Applies to aggregations but not windowing
        self._as_index = as_index

    def sum(self, *args, **kwargs) -> df.DataFrame:
        """Sums the numeric values for each group in the dataframe. Drops non-numeric columns always (like numeric_only=True in Pandas)."""
        aggregated_col_ids = self._aggregated_columns(numeric_only=True)
        return self._aggregate(agg_ops.sum_op, aggregated_col_ids)

    def mean(self, *args, **kwargs) -> df.DataFrame:
        """Calculates the mean of non-null values in each group. Drops non-numeric columns."""
        aggregated_col_ids = self._aggregated_columns(numeric_only=True)
        return self._aggregate(agg_ops.mean_op, aggregated_col_ids)

    def min(self, *args, **kwargs) -> df.DataFrame:
        """Calculates the minimum value in each group."""
        aggregated_col_ids = self._aggregated_columns(numeric_only=True)
        return self._aggregate(agg_ops.min_op, aggregated_col_ids)

    def max(self, *args, **kwargs) -> df.DataFrame:
        """Calculates the maximum value in each group."""
        aggregated_col_ids = self._aggregated_columns(numeric_only=True)
        return self._aggregate(agg_ops.max_op, aggregated_col_ids)

    def std(self, *args, **kwargs) -> df.DataFrame:
        """Calculates the standard deviation of values in each group. Drops non-numeric columns."""
        aggregated_col_ids = self._aggregated_columns(numeric_only=True)
        return self._aggregate(agg_ops.std_op, aggregated_col_ids)

    def var(self, *args, **kwargs) -> df.DataFrame:
        """Calculates the variance of values in each group. Drops non-numeric columns."""
        aggregated_col_ids = self._aggregated_columns(numeric_only=True)
        return self._aggregate(agg_ops.var_op, aggregated_col_ids)

    def all(self, *args, **kwargs) -> df.DataFrame:
        """Returns true if any non-null value evalutes to true for each group."""
        return self._aggregate(agg_ops.all_op, self._aggregated_columns())

    def any(self, *args, **kwargs) -> df.DataFrame:
        """Returns true if all non-null values evaluate to true for each group."""
        return self._aggregate(agg_ops.any_op, self._aggregated_columns())

    def count(self, *args, **kwargs) -> df.DataFrame:
        """Counts the non-null values in each group."""
        return self._aggregate(agg_ops.count_op, self._aggregated_columns())

    def _aggregated_columns(self, numeric_only: bool = False):
        return [
            col_id
            for col_id, dtype in zip(self._block.value_columns, self._block.dtypes)
            if col_id not in self._by_col_ids
            and (
                (not numeric_only)
                or (dtype in leanframe.dtypes.NUMERIC_BIGFRAMES_TYPES)
            )
        ]

    def _aggregate(
        self,
        aggregate_op: agg_ops.AggregateOp,
        aggregated_col_ids: typing.Sequence[str],
    ) -> df.DataFrame:
        aggregations = [
            (col_id, aggregate_op, col_id + "_bf_aggregated")
            for col_id in aggregated_col_ids
        ]

        result_block = self._block.aggregate(
            self._by_col_ids,
            aggregations,
            dropna=self._dropna,
        )
        if self._as_index:
            # Promote 'by' column to index.
            # TODO(tbergeron): generalize for multi-index (once multi-index introduced)
            by_col_id = self._by_col_ids[0]
            result_block.index_columns = self._by_col_ids
            index_label = self._col_id_labels[by_col_id]
            result_block.index.name = index_label
            labels = [self._col_id_labels[col_id] for col_id in aggregated_col_ids]
            return df.DataFrame(result_block.index, labels)
        else:
            result_block = result_block.reset_index()
            by_col_labels = [self._col_id_labels[col_id] for col_id in self._by_col_ids]
            aggregate_labels = [
                self._col_id_labels[col_id] for col_id in aggregated_col_ids
            ]
            return df.DataFrame(result_block.index, by_col_labels + aggregate_labels)
