"""Test the convenience join() method in NestedHandler."""

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
def sample_data(duckdb_backend):
    """Create sample test data with nested and flat tables.
    
    IMPORTANT: All tables use the SAME backend connection to ensure
    they can be joined within a single NestedHandler.
    """
    # Customers with nested profile
    customers_data = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'profile': [
            {'name': 'Alice', 'contact': {'email': 'alice@example.com', 'phone': '555-0001'}},
            {'name': 'Bob', 'contact': {'email': 'bob@example.com', 'phone': '555-0002'}},
            {'name': 'Charlie', 'contact': {'email': 'charlie@example.com', 'phone': '555-0003'}},
        ]
    })
    
    # Orders (flat)
    orders_data = pd.DataFrame({
        'order_id': [101, 102, 103, 104],
        'customer_id': [1, 2, 1, 3],
        'amount': [100.0, 200.0, 150.0, 75.0]
    })
    
    # Products (flat)
    products_data = pd.DataFrame({
        'product_id': [1, 2, 3],
        'name': ['Widget', 'Gadget', 'Doohickey'],
        'price': [29.99, 149.99, 9.99]
    })
    
    # Create all tables on the SAME backend
    customers_table = duckdb_backend.create_table('test_customers', customers_data, temp=True)
    orders_table = duckdb_backend.create_table('test_orders', orders_data, temp=True)
    products_table = duckdb_backend.create_table('test_products', products_data, temp=True)
    
    return {
        'backend': duckdb_backend,  # Include backend for tests that need it
        'customers': DataFrame(customers_table),
        'orders': DataFrame(orders_table),
        'products': DataFrame(products_table)
    }


def test_join_two_tables_simple(sample_data):
    """Test simple two-table inner join."""
    handler = NestedHandler()
    handler.add("customers", sample_data['customers'])
    handler.add("orders", sample_data['orders'])
    
    # Join on customer_id
    result = handler.join(
        tables={"c": "customers", "o": "orders"},
        on=[("c", "customer_id", "o", "customer_id")],
        how="inner"
    )
    
    # Verify result
    result_pd = result.to_pandas()
    assert len(result_pd) == 4  # 4 orders
    assert 'customer_id' in result_pd.columns
    assert 'order_id' in result_pd.columns
    assert 'amount' in result_pd.columns
    assert 'profile_name' in result_pd.columns  # Nested field extracted
    assert 'profile_contact_email' in result_pd.columns


def test_join_handles_nested_extraction(sample_data):
    """Test that join automatically extracts nested fields."""
    handler = NestedHandler()
    handler.add("customers", sample_data['customers'])
    handler.add("orders", sample_data['orders'])
    
    result = handler.join(
        tables={"c": "customers", "o": "orders"},
        on=[("c", "customer_id", "o", "customer_id")],
        how="left"
    )
    
    columns = result.columns
    
    # Check nested fields were extracted
    assert 'profile_name' in columns
    assert 'profile_contact_email' in columns
    assert 'profile_contact_phone' in columns
    
    # Check original nested column is gone
    assert 'profile' not in columns


def test_join_three_tables(sample_data):
    """Test three-table join."""
    # Add product_id to orders for this test
    orders_pd = sample_data['orders'].to_pandas()
    orders_pd['product_id'] = [1, 2, 1, 3]
    
    handler = NestedHandler()
    handler.add("customers", sample_data['customers'])
    
    # CRITICAL: Use the SAME backend from sample_data fixture
    # All DataFrames in one NestedHandler must share the same backend!
    backend = sample_data['backend']
    orders_table = backend.create_table('orders_with_products', orders_pd, temp=True)
    handler.add("orders", DataFrame(orders_table))
    handler.add("products", sample_data['products'])
    
    # Three-table join
    result = handler.join(
        tables={"c": "customers", "o": "orders", "p": "products"},
        on=[
            ("c", "customer_id", "o", "customer_id"),
            ("o", "product_id", "p", "product_id")
        ],
        how="inner"
    )
    
    # Verify all tables are joined
    result_pd = result.to_pandas()
    assert 'customer_id' in result_pd.columns
    assert 'order_id' in result_pd.columns
    assert 'product_id' in result_pd.columns
    assert 'profile_name' in result_pd.columns
    assert 'name' in result_pd.columns  # Product name
    assert 'price' in result_pd.columns


def test_join_different_types(sample_data):
    """Test different join types."""
    handler = NestedHandler()
    handler.add("customers", sample_data['customers'])
    handler.add("orders", sample_data['orders'])
    
    # Inner join
    inner = handler.join(
        tables={"c": "customers", "o": "orders"},
        on=[("c", "customer_id", "o", "customer_id")],
        how="inner"
    )
    inner_pd = inner.to_pandas()
    assert len(inner_pd) == 4  # Only customers with orders
    
    # Left join - all customers
    left = handler.join(
        tables={"c": "customers", "o": "orders"},
        on=[("c", "customer_id", "o", "customer_id")],
        how="left"
    )
    left_pd = left.to_pandas()
    # Should include all customers (3) × their orders
    # Customer 1 has 2 orders, customer 2 has 1, customer 3 has 1
    assert len(left_pd) == 4


def test_join_empty_tables_dict_raises_error(sample_data):
    """Test that empty tables dict raises error."""
    handler = NestedHandler()
    handler.add("customers", sample_data['customers'])
    
    with pytest.raises(ValueError, match="Must provide at least one table"):
        handler.join(tables={}, on=[], how="inner")


def test_join_missing_conditions_raises_error(sample_data):
    """Test that missing join conditions raises error for non-cross joins."""
    handler = NestedHandler()
    handler.add("customers", sample_data['customers'])
    handler.add("orders", sample_data['orders'])
    
    with pytest.raises(ValueError, match="Must provide either"):
        handler.join(
            tables={"c": "customers", "o": "orders"},
            how="inner"
        )


def test_join_nonexistent_dataframe_raises_error(sample_data):
    """Test that referencing nonexistent DataFrame raises error."""
    handler = NestedHandler()
    handler.add("customers", sample_data['customers'])
    
    with pytest.raises(KeyError, match="not found"):
        handler.join(
            tables={"c": "customers", "o": "nonexistent"},
            on=[("c", "customer_id", "o", "customer_id")],
            how="inner"
        )


def test_join_result_can_be_further_processed(sample_data):
    """Test that join result can be used for further Ibis operations."""
    handler = NestedHandler()
    handler.add("customers", sample_data['customers'])
    handler.add("orders", sample_data['orders'])
    
    # Join
    result = handler.join(
        tables={"c": "customers", "o": "orders"},
        on=[("c", "customer_id", "o", "customer_id")],
        how="inner"
    )
    
    # Continue with Ibis operations
    filtered = result._data.filter(result._data.amount > 100)
    filtered_df = DataFrame(filtered)
    
    filtered_pd = filtered_df.to_pandas()
    assert len(filtered_pd) == 2  # Only 2 orders > 100
    assert all(filtered_pd['amount'] > 100)


def test_join_multi_column_conditions(sample_data):
    """Test join with multiple column conditions."""
    # CRITICAL: Use the SAME backend from sample_data fixture
    backend = sample_data['backend']
    
    table1_data = pd.DataFrame({
        'id': [1, 2, 3],
        'region': ['US', 'EU', 'US'],
        'value': [10, 20, 30]
    })
    
    table2_data = pd.DataFrame({
        'id': [1, 2, 1],
        'region': ['US', 'EU', 'EU'],
        'amount': [100, 200, 150]
    })
    
    table1 = backend.create_table('t1', table1_data, temp=True)
    table2 = backend.create_table('t2', table2_data, temp=True)
    
    handler = NestedHandler()
    handler.add("t1", DataFrame(table1))
    handler.add("t2", DataFrame(table2))
    
    # Multi-column join
    result = handler.join(
        tables={"a": "t1", "b": "t2"},
        on=[
            ("a", "id", "b", "id"),
            ("a", "region", "b", "region")
        ],
        how="inner"
    )
    
    result_pd = result.to_pandas()
    # Should only match rows where BOTH id AND region match
    assert len(result_pd) == 2  # (1, US) and (2, EU)


def test_join_cross_join(sample_data):
    """Test cross join (Cartesian product)."""
    handler = NestedHandler()
    handler.add("customers", sample_data['customers'])
    handler.add("products", sample_data['products'])
    
    # Cross join - no conditions needed
    result = handler.join(
        tables={"c": "customers", "p": "products"},
        how="cross"
    )
    
    result_pd = result.to_pandas()
    # 3 customers × 3 products = 9 rows
    assert len(result_pd) == 9
