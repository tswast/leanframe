"""
Unit tests for indexing functionality in leanframe.

Tests cover:
- Index creation and properties
- .iloc position-based indexing
- .loc label-based indexing
- .head() and .tail() methods
- Integration with nested data handling
- Error handling
"""

import pytest
import ibis
import pandas as pd
from datetime import datetime

from leanframe.core.frame import DataFrame
from leanframe.core.indexing import Index


class TestIndex:
    """Tests for the Index class."""
    
    def test_index_creation(self):
        """Test creating an Index."""
        idx = Index('customer_id', ascending=True)
        assert idx.column == 'customer_id'
        assert idx.columns == ['customer_id']
        assert idx.ascending == [True]
    
    def test_index_with_name(self):
        """Test creating an Index with custom name."""
        idx = Index('timestamp', ascending=False, name='time_idx')
        assert idx.column == 'timestamp'
        assert idx.columns == ['timestamp']
        assert idx.ascending == [False]
        assert idx.name == 'time_idx'
    
    def test_index_repr(self):
        """Test Index string representation."""
        idx = Index('id', ascending=True)
        assert 'Index' in repr(idx)
        assert 'id' in repr(idx)
        assert 'ascending' in repr(idx)
    
    def test_multi_column_index(self):
        """Test creating a multi-column Index."""
        idx = Index(['priority', 'timestamp'], ascending=[False, True])
        assert idx.columns == ['priority', 'timestamp']
        assert idx.ascending == [False, True]
        assert idx.column == 'priority'  # First column
        assert idx.is_multi_column() is True
    
    def test_multi_column_index_single_ascending(self):
        """Test multi-column index with single ascending value."""
        idx = Index(['col1', 'col2', 'col3'], ascending=False)
        assert idx.columns == ['col1', 'col2', 'col3']
        assert idx.ascending == [False, False, False]  # Applied to all
    
    def test_multi_column_index_mismatch_error(self):
        """Test that mismatched ascending list raises error."""
        with pytest.raises(ValueError, match="Length of ascending"):
            Index(['col1', 'col2'], ascending=[True, False, True])


class TestSetIndex:
    """Tests for DataFrame.set_index() method."""
    
    @pytest.fixture
    def simple_df(self):
        """Create a simple DataFrame for testing."""
        data = {
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50],
            'name': ['a', 'b', 'c', 'd', 'e']
        }
        ibis_table = ibis.memtable(data)
        return DataFrame(ibis_table)
    
    def test_set_index_basic(self, simple_df):
        """Test setting index on a column."""
        df = simple_df.set_index('id')
        assert df.index is not None
        assert df.index.column == 'id'
        assert df.index.columns == ['id']
        assert df.index.ascending == [True]
    
    def test_set_index_descending(self, simple_df):
        """Test setting index with descending order."""
        df = simple_df.set_index('value', ascending=False)
        assert df.index is not None
        assert df.index.column == 'value'
        assert df.index.columns == ['value']
        assert df.index.ascending == [False]
    
    def test_set_index_with_name(self, simple_df):
        """Test setting index with custom name."""
        df = simple_df.set_index('id', name='custom_idx')
        assert df.index.name == 'custom_idx'
    
    def test_set_index_invalid_column(self, simple_df):
        """Test setting index on non-existent column raises error."""
        with pytest.raises(KeyError):
            simple_df.set_index('nonexistent')
    
    def test_set_index_returns_new_df(self, simple_df):
        """Test that set_index returns a new DataFrame."""
        df_indexed = simple_df.set_index('id')
        # Original should not have index
        assert simple_df.index is None
        # New one should have index
        assert df_indexed.index is not None
    
    def test_set_index_multi_column(self, simple_df):
        """Test setting index on multiple columns."""
        df = simple_df.set_index(['id', 'value'], ascending=[True, False])
        assert df.index is not None
        assert df.index.columns == ['id', 'value']
        assert df.index.ascending == [True, False]
        assert df.index.is_multi_column() is True


class TestILocIndexing:
    """Tests for .iloc position-based indexing."""
    
    @pytest.fixture
    def indexed_df(self):
        """Create an indexed DataFrame for testing."""
        data = {
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50]
        }
        ibis_table = ibis.memtable(data)
        df = DataFrame(ibis_table)
        return df.set_index('id', ascending=True)
    
    def test_iloc_single_row(self, indexed_df):
        """Test getting a single row with .iloc."""
        result = indexed_df.iloc[0]
        assert isinstance(result, DataFrame)
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 1
        assert pandas_result['id'].iloc[0] == 1
    
    def test_iloc_slice(self, indexed_df):
        """Test slicing with .iloc."""
        result = indexed_df.iloc[1:3]
        assert isinstance(result, DataFrame)
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 2
        assert list(pandas_result['id']) == [2, 3]
    
    def test_iloc_open_ended_slice(self, indexed_df):
        """Test open-ended slice with .iloc."""
        result = indexed_df.iloc[3:]
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 2
        assert list(pandas_result['id']) == [4, 5]
    
    def test_iloc_without_index_raises_error(self):
        """Test that .iloc without index raises ValueError."""
        data = {'id': [1, 2, 3]}
        ibis_table = ibis.memtable(data)
        df = DataFrame(ibis_table)
        
        with pytest.raises(ValueError, match="Cannot use .iloc without an index"):
            df.iloc[0]
    
    def test_iloc_descending_order(self):
        """Test .iloc with descending index order."""
        data = {
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50]
        }
        ibis_table = ibis.memtable(data)
        df = DataFrame(ibis_table).set_index('id', ascending=False)
        
        # With descending order, first row should be id=5
        result = df.iloc[0]
        pandas_result = result.to_pandas()
        assert pandas_result['id'].iloc[0] == 5
    
    def test_iloc_multi_column_ordering(self):
        """Test .iloc with multi-column index."""
        data = {
            'priority': [1, 1, 2, 2, 3],
            'timestamp': [5, 3, 4, 2, 1],
            'value': [10, 20, 30, 40, 50]
        }
        ibis_table = ibis.memtable(data)
        # Order by priority DESC, then timestamp ASC
        df = DataFrame(ibis_table).set_index(
            ['priority', 'timestamp'], 
            ascending=[False, True]
        )
        
        # First row should be priority=3, timestamp=1
        result = df.iloc[0]
        pandas_result = result.to_pandas()
        assert pandas_result['priority'].iloc[0] == 3
        assert pandas_result['timestamp'].iloc[0] == 1
        assert pandas_result['value'].iloc[0] == 50


class TestLocIndexing:
    """Tests for .loc label-based indexing."""
    
    @pytest.fixture
    def indexed_df(self):
        """Create an indexed DataFrame for testing."""
        data = {
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50]
        }
        ibis_table = ibis.memtable(data)
        df = DataFrame(ibis_table)
        return df.set_index('id')
    
    def test_loc_single_value(self, indexed_df):
        """Test getting rows by single value with .loc."""
        result = indexed_df.loc[3]
        assert isinstance(result, DataFrame)
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 1
        assert pandas_result['id'].iloc[0] == 3
        assert pandas_result['value'].iloc[0] == 30
    
    def test_loc_range(self, indexed_df):
        """Test range query with .loc."""
        result = indexed_df.loc[2:4]
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 3
        assert list(pandas_result['id']) == [2, 3, 4]
    
    def test_loc_list_of_values(self, indexed_df):
        """Test getting multiple specific values with .loc."""
        result = indexed_df.loc[[1, 3, 5]]
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 3
        assert set(pandas_result['id']) == {1, 3, 5}
    
    def test_loc_without_index_raises_error(self):
        """Test that .loc without index raises ValueError."""
        data = {'id': [1, 2, 3]}
        ibis_table = ibis.memtable(data)
        df = DataFrame(ibis_table)
        
        with pytest.raises(ValueError, match="Cannot use .loc without an index"):
            df.loc[1]


class TestHeadTail:
    """Tests for .head() and .tail() methods."""
    
    @pytest.fixture
    def indexed_df(self):
        """Create an indexed DataFrame for testing."""
        data = {
            'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'value': list(range(10, 110, 10))
        }
        ibis_table = ibis.memtable(data)
        df = DataFrame(ibis_table)
        return df.set_index('id', ascending=True)
    
    def test_head_default(self, indexed_df):
        """Test .head() with default n=5."""
        result = indexed_df.head()
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 5
        assert list(pandas_result['id']) == [1, 2, 3, 4, 5]
    
    def test_head_custom_n(self, indexed_df):
        """Test .head() with custom n."""
        result = indexed_df.head(3)
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 3
        assert list(pandas_result['id']) == [1, 2, 3]
    
    def test_tail_default(self, indexed_df):
        """Test .tail() with default n=5."""
        result = indexed_df.tail()
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 5
        assert list(pandas_result['id']) == [6, 7, 8, 9, 10]
    
    def test_tail_custom_n(self, indexed_df):
        """Test .tail() with custom n."""
        result = indexed_df.tail(3)
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 3
        assert list(pandas_result['id']) == [8, 9, 10]
    
    def test_tail_without_index_raises_error(self):
        """Test that .tail() without index raises ValueError."""
        data = {'id': [1, 2, 3]}
        ibis_table = ibis.memtable(data)
        df = DataFrame(ibis_table)
        
        with pytest.raises(ValueError, match="Cannot use .tail\\(\\) without an index"):
            df.tail()
    
    def test_head_without_index(self):
        """Test that .head() works without index (arbitrary order)."""
        data = {'id': [1, 2, 3, 4, 5]}
        ibis_table = ibis.memtable(data)
        df = DataFrame(ibis_table)
        
        # Should work but order is arbitrary
        result = df.head(3)
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 3


class TestIndexingWithNestedData:
    """Tests for indexing with nested column handling."""
    
    @pytest.fixture
    def nested_df(self):
        """Create a DataFrame with nested data."""
        data = {
            'id': [1, 2, 3],
            'person': [
                {'name': 'Alice', 'age': 30},
                {'name': 'Bob', 'age': 25},
                {'name': 'Carol', 'age': 35}
            ]
        }
        ibis_table = ibis.memtable(data)
        return DataFrame(ibis_table)
    
    def test_set_index_on_regular_column(self, nested_df):
        """Test setting index on a regular (non-nested) column."""
        df = nested_df.set_index('id')
        assert df.index.column == 'id'
    
    def test_extract_then_index(self, nested_df):
        """Test extracting nested fields then applying indexing."""
        from leanframe.core.frame import DataFrameHandler
        
        # Extract nested fields
        handler = DataFrameHandler(nested_df)
        flat_df = handler.extract_nested_fields(verbose=False)
        
        # Now index on extracted field
        df_indexed = flat_df.set_index('person_age', ascending=False)
        
        # Get oldest person
        result = df_indexed.iloc[0]
        pandas_result = result.to_pandas()
        assert pandas_result['person_name'].iloc[0] == 'Carol'
        assert pandas_result['person_age'].iloc[0] == 35


class TestIndexingEdgeCases:
    """Tests for edge cases and error conditions."""
    
    def test_empty_dataframe(self):
        """Test indexing on empty DataFrame."""
        data = {'id': [], 'value': []}
        ibis_table = ibis.memtable(data)
        df = DataFrame(ibis_table).set_index('id')
        
        # Should not error, just return empty
        result = df.head()
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 0
    
    def test_single_row_dataframe(self):
        """Test indexing on single row DataFrame."""
        data = {'id': [1], 'value': [10]}
        ibis_table = ibis.memtable(data)
        df = DataFrame(ibis_table).set_index('id')
        
        result = df.iloc[0]
        pandas_result = result.to_pandas()
        assert len(pandas_result) == 1
        assert pandas_result['id'].iloc[0] == 1
    
    def test_chaining_operations(self):
        """Test chaining set_index with other operations."""
        data = {
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50]
        }
        ibis_table = ibis.memtable(data)
        df = DataFrame(ibis_table)
        
        # Chain: set_index -> head -> to_pandas
        result = df.set_index('id').head(3).to_pandas()
        assert len(result) == 3
        assert list(result['id']) == [1, 2, 3]


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
