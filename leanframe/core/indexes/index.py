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

"""An index based on a single column."""

from __future__ import annotations

import typing
from typing import Callable, Optional, Tuple

import ibis.expr.types as ibis_types
import pandas as pd

import leanframe.core.blocks as blocks
import leanframe.core.indexes.implicitjoiner as implicitjoiner
import leanframe.core.joins as joins


class Index(implicitjoiner.ImplicitJoiner):
    """An index based on a single column."""

    # TODO(swast): Handle more than 1 index column, possibly in a separate
    # MultiIndex class.
    # TODO(swast): Include ordering here?
    def __init__(
        self, block: blocks.Block, index_column: str, name: Optional[str] = None
    ):
        super().__init__(block, name=name)
        self._index_column = index_column

    def __repr__(self) -> str:
        """Converts an Index to a string."""
        # TODO(swast): Add a timeout here? If the query is taking a long time,
        # maybe we just print the job metadata that we have so far?
        # TODO(swast): Avoid downloading the whole index by using job
        # metadata, like we do with DataFrame.
        preview = self.compute()
        return repr(preview)

    def compute(self) -> pd.Index:
        """Executes deferred operations and downloads the results."""
        # Project down to only the index column. So the query can be cached to visualize other data.
        expr = self._expr.projection([self._expr.get_any_column(self._index_column)])
        df = (
            expr.start_query()
            .result()
            .to_dataframe(
                bool_dtype=pd.BooleanDtype(),
                int_dtype=pd.Int64Dtype(),
                float_dtype=pd.Float64Dtype(),
                string_dtype=pd.StringDtype(storage="pyarrow"),
            )
        )
        df.set_index(self._index_column)
        index = df.index
        index.name = self._name
        return index

    def copy(self) -> Index:
        """Make a copy of this object."""
        # TODO(swast): Should this make a copy of block?
        return Index(self._block, self._index_column, name=self.name)

    def join(
        self, other: implicitjoiner.ImplicitJoiner, *, how="left", sort=False
    ) -> Tuple[
        Index,
        Tuple[Callable[[str], ibis_types.Value], Callable[[str], ibis_types.Value]],
    ]:
        if not isinstance(other, Index):
            # TODO(swast): We need to improve this error message to be more
            # actionable for the user. For example, it's possible they
            # could call set_index and try again to resolve this error.
            raise ValueError(
                "Can't mixed objects with explicit Index and ImpliedJoiner"
            )

        # TODO(swast): Support cross-joins (requires reindexing).
        if how not in {"outer", "left", "right", "inner"}:
            raise NotImplementedError(
                "Only how='outer','left','right','inner' currently supported"
            )

        (
            combined_expr,
            joined_index_col_name,
            (get_column_left, get_column_right),
        ) = joins.join_by_column(
            self._block.expr,
            self._index_column,
            other._block.expr,
            other._index_column,
            how=how,
            sort=sort,
        )
        block = blocks.Block(combined_expr)
        block.index_columns = [joined_index_col_name]
        combined_index = typing.cast(Index, block.index)
        combined_index.name = self.name if self.name == other.name else None
        return (
            combined_index,
            (get_column_left, get_column_right),
        )
