"""Test the new prepare() method in NestedHandler."""

import pytest
import ibis
import pandas as pd

from leanframe.core.frame import DataFrame
from leanframe.core.nested_handler import NestedHandler


@pytest.fixture
def duckdb_backend():
    """Create a DuckDB backend for testing."""
    return ibis.duckdb.connect()


@pytest.fixture
def nested_customers_df(duckdb_backend):
    """Create a DataFrame with nested customer data."""
    data = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'profile': [
            {'name': 'Alice', 'contact': {'email': 'alice@example.com', 'phone': '555-0001'}},
            {'name': 'Bob', 'contact': {'email': 'bob@example.com', 'phone': '555-0002'}},
            {'name': 'Charlie', 'contact': {'email': 'charlie@example.com', 'phone': '555-0003'}},
        ]
    })
    
    table = duckdb_backend.create_table('test_customers', data, temp=True)
    return DataFrame(table)


def test_prepare_extracts_all_nested_fields(nested_customers_df):
    """Test that prepare() extracts all nested fields by default."""
    handler = NestedHandler()
    handler.add("customers", nested_customers_df)
    
    # Prepare should extract all nested fields
    prepared = handler.prepare("customers")
    
    # Check that nested fields were extracted
    columns = prepared.columns
    assert 'customer_id' in columns  # Original flat column
    assert 'profile_name' in columns  # Extracted from profile.name
    assert 'profile_contact_email' in columns  # Extracted from profile.contact.email
    assert 'profile_contact_phone' in columns  # Extracted from profile.contact.phone
    
    # Original nested column should not be in prepared DataFrame
    assert 'profile' not in columns


def test_prepare_with_specific_fields(nested_customers_df):
    """Test that prepare() can extract specific fields only."""
    handler = NestedHandler()
    handler.add("customers", nested_customers_df)
    
    # Prepare with specific fields
    prepared = handler.prepare(
        "customers",
        fields=['profile.contact.email', 'profile.name']
    )
    
    columns = prepared.columns
    
    # Should have requested fields
    assert 'profile_contact_email' in columns
    assert 'profile_name' in columns
    
    # Should have original flat columns
    assert 'customer_id' in columns
    
    # Should NOT have unrequested nested fields
    assert 'profile_contact_phone' not in columns


def test_prepare_nonexistent_field_raises_error(nested_customers_df):
    """Test that prepare() raises error for nonexistent fields."""
    handler = NestedHandler()
    handler.add("customers", nested_customers_df)
    
    with pytest.raises(ValueError, match="not found in nested structure"):
        handler.prepare("customers", fields=['profile.nonexistent.field'])


def test_prepare_nonexistent_dataframe_raises_error():
    """Test that prepare() raises error for nonexistent DataFrame."""
    handler = NestedHandler()
    
    with pytest.raises(KeyError, match="not found"):
        handler.prepare("nonexistent")


def test_prepare_does_not_modify_original(nested_customers_df):
    """Test that prepare() doesn't modify the original DataFrame in handler."""
    handler = NestedHandler()
    handler.add("customers", nested_customers_df)
    
    # Get original handler
    original_handler = handler.get("customers")
    original_columns = original_handler.original_columns
    
    # Prepare
    prepared = handler.prepare("customers")
    
    # Original should be unchanged
    assert original_handler.original_columns == original_columns
    assert 'profile' in original_handler.original_columns
    
    # Prepared should be different
    assert 'profile' not in prepared.columns


def test_prepare_enables_direct_ibis_joins(nested_customers_df, duckdb_backend):
    """Test that prepared DataFrames can be joined using direct Ibis operations."""
    # Create orders DataFrame
    orders_data = pd.DataFrame({
        'order_id': [101, 102, 103],
        'customer_email': ['alice@example.com', 'bob@example.com', 'alice@example.com'],
        'amount': [100.0, 200.0, 150.0]
    })
    orders_table = duckdb_backend.create_table('test_orders', orders_data, temp=True)
    orders_df = DataFrame(orders_table)
    
    # Add both to handler
    handler = NestedHandler()
    handler.add("customers", nested_customers_df)
    handler.add("orders", orders_df)
    
    # Prepare both
    customers_flat = handler.prepare("customers")
    orders_flat = handler.prepare("orders")
    
    # Join using direct Ibis operations
    joined = customers_flat._data.join(
        orders_flat._data,
        predicates=[
            customers_flat._data.profile_contact_email == orders_flat._data.customer_email
        ],
        how="inner"
    )
    
    result = DataFrame(joined)
    
    # Verify join worked
    result_pd = result.to_pandas()
    assert len(result_pd) == 3  # 3 orders
    assert 'customer_id' in result_pd.columns
    assert 'order_id' in result_pd.columns
    assert 'amount' in result_pd.columns
    assert 'profile_contact_email' in result_pd.columns


def test_prepare_with_flat_dataframe(duckdb_backend):
    """Test that prepare() works with already-flat DataFrames."""
    # Create flat DataFrame (no nested columns)
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    
    table = duckdb_backend.create_table('test_flat', data, temp=True)
    flat_df = DataFrame(table)
    
    handler = NestedHandler()
    handler.add("flat", flat_df)
    
    # Prepare should work even without nested fields
    prepared = handler.prepare("flat")
    
    # Should have all original columns
    columns = prepared.columns
    assert 'id' in columns
    assert 'name' in columns
    assert 'age' in columns
