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

"""
Indexing support for leanframe DataFrames.

This module provides pandas-like indexing with SQL semantics. Since BigQuery
and other SQL databases don't have persistent row ordering, we use explicit
ORDER BY clauses to establish deterministic ordering for subset selection.

Key Design Principles:
- Single index only (no multi-index support)
- Explicit ordering specification via column(s) + sort direction
- .loc and .iloc style accessors that translate to SQL
- Integration with nested column handling
- Deferred execution (no materialization unless needed)

Philosophy:
    Pandas: df.iloc[0:10] assumes row order is intrinsic
    Leanframe: df.set_index('timestamp', 'desc').iloc[0:10] makes ordering explicit
    
    This forces users to think about ordering, which is critical for
    reproducible results in SQL databases without implicit row ordering.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from leanframe.core.frame import DataFrame

import ibis
import ibis.expr.types as ibis_types


class Index:
    """
    Represents an ordering specification for a DataFrame.
    
    Unlike pandas Index which stores actual values, leanframe Index is a
    specification for how to order rows deterministically. This allows
    SQL-based .loc and .iloc operations.
    
    Design:
        - Stores column name(s) and sort direction(s)
        - Does NOT materialize data
        - Used by Indexer classes to build ORDER BY clauses
        - Supports single or multiple columns (composite ordering)
    
    Attributes:
        columns: List of column names to order by (in priority order)
        ascending: List of sort directions (True=ASC, False=DESC) for each column
        name: Optional name for the index
    
    Example:
        # Single column index
        idx = Index('timestamp', ascending=False)
        
        # Multi-column index (ORDER BY priority DESC, timestamp DESC)
        idx = Index(['priority', 'timestamp'], ascending=[False, False])
        
        # With custom name
        idx = Index('customer_id', ascending=True, name='customer_idx')
    """
    
    def __init__(
        self,
        columns: str | list[str],
        ascending: bool | list[bool] = True,
        name: str | None = None
    ):
        """
        Initialize an Index specification.
        
        Args:
            columns: Column name(s) to order by. Can be:
                    - Single string: 'timestamp'
                    - List of strings: ['priority', 'timestamp']
            ascending: Sort direction(s). Can be:
                      - Single bool: True (applies to all columns)
                      - List of bools: [False, True] (one per column)
            name: Optional name for the index
            
        Raises:
            ValueError: If ascending list length doesn't match columns list length
        """
        # Normalize to lists
        if isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = list(columns)
        
        # Normalize ascending to list
        if isinstance(ascending, bool):
            self.ascending = [ascending] * len(self.columns)
        else:
            self.ascending = list(ascending)
            if len(self.ascending) != len(self.columns):
                raise ValueError(
                    f"Length of ascending ({len(self.ascending)}) must match "
                    f"length of columns ({len(self.columns)})"
                )
        
        # Set name
        if name is not None:
            self.name = name
        elif len(self.columns) == 1:
            self.name = self.columns[0]
        else:
            self.name = f"({', '.join(self.columns)})"
    
    @property
    def column(self) -> str:
        """
        Get the primary (first) column name.
        
        For backward compatibility with single-column index usage.
        
        Returns:
            First column in the index
        """
        return self.columns[0]
    
    def is_multi_column(self) -> bool:
        """Check if this is a multi-column index."""
        return len(self.columns) > 1
    
    def __repr__(self) -> str:
        if len(self.columns) == 1:
            direction = "ascending" if self.ascending[0] else "descending"
            return f"Index('{self.columns[0]}', {direction})"
        else:
            parts = []
            for col, asc in zip(self.columns, self.ascending):
                direction = "ASC" if asc else "DESC"
                parts.append(f"{col} {direction}")
            return f"Index([{', '.join(parts)}])"
    
    def __str__(self) -> str:
        parts = []
        for col, asc in zip(self.columns, self.ascending):
            direction = "ASC" if asc else "DESC"
            parts.append(f"{col} {direction}")
        return ", ".join(parts)


class ILocIndexer:
    """
    Position-based indexing with explicit ordering (like pandas .iloc).
    
    Since SQL databases don't have intrinsic row positions, we require
    the DataFrame to have an explicit index (ORDER BY specification).
    
    Supported operations:
        - df.iloc[5] - Single row by position
        - df.iloc[5:10] - Slice of rows
        - df.iloc[[1, 3, 5]] - Specific positions
        - df.iloc[:10] - First N rows (with ordering)
        - df.iloc[-10:] - Last N rows (with ordering)
    
    Example:
        # Must set index first for deterministic ordering
        df = df.set_index('timestamp', ascending=False)
        
        # Now position-based indexing works
        first_row = df.iloc[0]  # Newest record (timestamp DESC)
        top_10 = df.iloc[0:10]  # 10 newest records
        last_row = df.iloc[-1]  # Oldest record
    """
    
    def __init__(self, dataframe: DataFrame):
        """
        Initialize iloc indexer.
        
        Args:
            dataframe: Parent DataFrame
        """
        self._df = dataframe
    
    def __getitem__(self, key):
        """
        Get rows by position.
        
        Args:
            key: int, slice, or list of ints
            
        Returns:
            DataFrame (for slices/lists) or Series (for single int)
            
        Raises:
            ValueError: If DataFrame has no index set
        """
        # Check if index is set
        if not hasattr(self._df, '_index') or self._df._index is None:
            raise ValueError(
                "Cannot use .iloc without an index. "
                "Use .set_index('column_name') to establish ordering first.\n\n"
                "Example: df.set_index('timestamp', ascending=False).iloc[0:10]"
            )
        
        index = self._df._index
        ibis_table = self._df._data
        
        # Apply ordering based on index (supports multi-column)
        order_exprs = []
        for col, asc in zip(index.columns, index.ascending):
            if asc:
                order_exprs.append(ibis_table[col])
            else:
                order_exprs.append(ibis_table[col].desc())
        ordered = ibis_table.order_by(order_exprs)
        
        # Handle different key types
        if isinstance(key, int):
            # Single row - use limit + offset
            if key < 0:
                # Negative indexing - need to reverse order and count from end
                # This is expensive in SQL but supported for pandas compatibility
                import warnings
                warnings.warn(
                    "Negative indexing with .iloc requires counting all rows. "
                    "Consider using positive indices for better performance.",
                    UserWarning
                )
                # Reverse order and take abs(key) - 1 offset
                # Reverse all ordering directions
                reverse_exprs = []
                for col, asc in zip(index.columns, index.ascending):
                    if asc:
                        reverse_exprs.append(ibis_table[col].desc())
                    else:
                        reverse_exprs.append(ibis_table[col])
                reversed_order = ibis_table.order_by(reverse_exprs)
                result = reversed_order.limit(1, offset=abs(key) - 1)
            else:
                result = ordered.limit(1, offset=key)
            
            # Return Series for single row
            from leanframe.core.frame import DataFrame
            from leanframe.core.series import Series
            temp_df = DataFrame(result)
            # Convert single row to Series - use first column as example
            # In practice, this returns a Series-like dict or the row
            # For now, return DataFrame (pandas also returns Series here)
            return temp_df
            
        elif isinstance(key, slice):
            # Slice - convert to limit/offset
            start = key.start or 0
            stop = key.stop
            step = key.step
            
            if step is not None and step != 1:
                raise NotImplementedError(
                    f"Step size {step} not supported. "
                    "SQL databases don't support stepping in result sets."
                )
            
            # Handle negative indices
            if start < 0 or (stop is not None and stop < 0):
                raise NotImplementedError(
                    "Negative indices in slices not yet supported. "
                    "Use positive indices: df.iloc[0:10]"
                )
            
            # Build SQL LIMIT/OFFSET
            if stop is None:
                # Open-ended slice: df.iloc[10:]
                result = ordered.limit(None, offset=start)
            else:
                # Bounded slice: df.iloc[10:20]
                limit = stop - start
                result = ordered.limit(limit, offset=start)
            
            from leanframe.core.frame import DataFrame
            return DataFrame(result)
            
        elif isinstance(key, list):
            # List of positions - need to use row_number() window function
            # This is more complex in SQL
            raise NotImplementedError(
                "List indexing with .iloc not yet supported. "
                "Use slices or single positions instead."
            )
        
        else:
            raise TypeError(
                f"Invalid index type: {type(key)}. "
                "Use int, slice, or list of ints."
            )


class LocIndexer:
    """
    Label-based indexing (like pandas .loc).
    
    Since leanframe focuses on SQL semantics, .loc operates on the index
    column values rather than traditional pandas labels.
    
    Supported operations:
        - df.loc[value] - Rows where index column == value
        - df.loc[value1:value2] - Range query on index column
        - df.loc[[val1, val2]] - Multiple specific values
    
    Example:
        # Set index on customer_id
        df = df.set_index('customer_id')
        
        # Get customer with ID 12345
        customer = df.loc[12345]
        
        # Get customers in ID range
        customers = df.loc[10000:20000]
        
        # Get specific customers
        customers = df.loc[[12345, 67890, 11111]]
    """
    
    def __init__(self, dataframe: DataFrame):
        """
        Initialize loc indexer.
        
        Args:
            dataframe: Parent DataFrame
        """
        self._df = dataframe
    
    def __getitem__(self, key):
        """
        Get rows by index label.
        
        Args:
            key: Single value, slice, or list of values
            
        Returns:
            DataFrame (for slices/lists) or filtered result
            
        Raises:
            ValueError: If DataFrame has no index set
        """
        # Check if index is set
        if not hasattr(self._df, '_index') or self._df._index is None:
            raise ValueError(
                "Cannot use .loc without an index. "
                "Use .set_index('column_name') first.\n\n"
                "Example: df.set_index('customer_id').loc[12345]"
            )
        
        index = self._df._index
        ibis_table = self._df._data
        
        # For multi-column index, use the primary (first) column for filtering
        # This is consistent with pandas behavior
        index_col = ibis_table[index.columns[0]]
        
        # Handle different key types
        if isinstance(key, slice):
            # Range query: df.loc[start:stop]
            start = key.start
            stop = key.stop
            
            if start is None and stop is None:
                # No filtering
                result = ibis_table
            elif start is None:
                # df.loc[:stop]
                result = ibis_table.filter(index_col <= stop)
            elif stop is None:
                # df.loc[start:]
                result = ibis_table.filter(index_col >= start)
            else:
                # df.loc[start:stop]
                result = ibis_table.filter(
                    (index_col >= start) & (index_col <= stop)
                )
            
            # Apply ordering
            if index.ascending:
                result = result.order_by(index_col)
            else:
                result = result.order_by(index_col.desc())
            
            from leanframe.core.frame import DataFrame
            return DataFrame(result)
            
        elif isinstance(key, (list, tuple)):
            # Multiple values: df.loc[[val1, val2, val3]]
            result = ibis_table.filter(index_col.isin(key))
            
            # Apply ordering (all index columns)
            order_exprs = []
            for col, asc in zip(index.columns, index.ascending):
                if asc:
                    order_exprs.append(result[col])
                else:
                    order_exprs.append(result[col].desc())
            result = result.order_by(order_exprs)
            
            from leanframe.core.frame import DataFrame
            return DataFrame(result)
            
        else:
            # Single value: df.loc[value]
            result = ibis_table.filter(index_col == key)
            
            from leanframe.core.frame import DataFrame
            return DataFrame(result)


class HeadTailMixin:
    """
    Mixin providing .head() and .tail() methods.
    
    These are convenience methods that wrap .iloc with explicit ordering.
    
    This mixin expects to be mixed into a class that has:
    - _data: ibis_types.Table (the underlying Ibis table)
    - _index: Index | None (the ordering specification)
    
    Typically used with DataFrame class.
    """
    
    # Type hints for attributes that must exist in the mixed class
    _data: ibis_types.Table
    _index: Index | None
    
    def head(self, n: int = 5) -> DataFrame:
        """
        Return first n rows based on index ordering.
        
        If no index is set, returns first n rows in arbitrary order
        (database dependent).
        
        Args:
            n: Number of rows to return (default: 5)
            
        Returns:
            DataFrame with first n rows
            
        Example:
            # With explicit ordering
            df.set_index('timestamp', ascending=False).head(10)
            
            # Without index (arbitrary order)
            df.head()  # First 5 rows in database order
        """
        ibis_table = self._data
        
        # Apply ordering if index is set (supports multi-column)
        if hasattr(self, '_index') and self._index is not None:
            order_exprs = []
            for col, asc in zip(self._index.columns, self._index.ascending):
                if asc:
                    order_exprs.append(ibis_table[col])
                else:
                    order_exprs.append(ibis_table[col].desc())
            ibis_table = ibis_table.order_by(order_exprs)
        
        result = ibis_table.limit(n)
        from leanframe.core.frame import DataFrame
        return DataFrame(result)
    
    def tail(self, n: int = 5) -> DataFrame:
        """
        Return last n rows based on index ordering.
        
        Requires index to be set for deterministic results.
        
        Args:
            n: Number of rows to return (default: 5)
            
        Returns:
            DataFrame with last n rows
            
        Raises:
            ValueError: If no index is set
            
        Example:
            df.set_index('timestamp', ascending=False).tail(10)
        """
        if not hasattr(self, '_index') or self._index is None:
            raise ValueError(
                "Cannot use .tail() without an index. "
                "Use .set_index('column_name') to establish ordering first.\n\n"
                "Example: df.set_index('timestamp').tail(10)"
            )
        
        ibis_table = self._data
        
        # Reverse the ordering to get last n rows (all columns)
        reverse_exprs = []
        for col, asc in zip(self._index.columns, self._index.ascending):
            if asc:
                reverse_exprs.append(ibis_table[col].desc())
            else:
                reverse_exprs.append(ibis_table[col])
        reversed_table = ibis_table.order_by(reverse_exprs)
        
        # Take n rows, then reverse back to original order
        result = reversed_table.limit(n)
        
        # Re-apply original ordering
        original_exprs = []
        for col, asc in zip(self._index.columns, self._index.ascending):
            if asc:
                original_exprs.append(result[col])
            else:
                original_exprs.append(result[col].desc())
        result = result.order_by(original_exprs)
        
        from leanframe.core.frame import DataFrame
        return DataFrame(result)


# Export public API
__all__ = [
    'Index',
    'ILocIndexer',
    'LocIndexer',
    'HeadTailMixin',
]
