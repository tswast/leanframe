"""
Example: Using Indexing with Nested Data in Leanframe

This demo shows how to use the new indexing features with nested columns.
It covers:
- Setting up a DataFrame with nested structures
- Using DataFrameHandler to extract nested fields
- Applying indexing for deterministic ordering
- Combining indexing with joins via NestedHandler
"""

import ibis
import pandas as pd
from datetime import datetime, timedelta

# Import leanframe components
from leanframe.core.frame import DataFrame, DataFrameHandler
from leanframe.core.nested_handler import NestedHandler


def create_sample_nested_data():
    """Create sample data with nested structures for testing."""
    
    # Sample customer data with nested profiles
    customers_data = {
        'customer_id': [1001, 1002, 1003, 1004, 1005],
        'profile': [
            {'name': 'Alice Johnson', 'age': 34, 'email': 'alice@example.com'},
            {'name': 'Bob Smith', 'age': 28, 'email': 'bob@example.com'},
            {'name': 'Carol Davis', 'age': 45, 'email': 'carol@example.com'},
            {'name': 'David Wilson', 'age': 31, 'email': 'david@example.com'},
            {'name': 'Eve Martinez', 'age': 39, 'email': 'eve@example.com'},
        ],
        'registration_date': [
            datetime(2024, 1, 15),
            datetime(2024, 2, 20),
            datetime(2023, 11, 5),
            datetime(2024, 3, 10),
            datetime(2023, 12, 18),
        ]
    }
    
    # Sample order data
    orders_data = {
        'order_id': [5001, 5002, 5003, 5004, 5005, 5006],
        'customer_id': [1001, 1001, 1002, 1003, 1004, 1005],
        'amount': [299.99, 149.50, 599.00, 89.99, 450.00, 199.99],
        'order_date': [
            datetime(2024, 3, 1),
            datetime(2024, 3, 15),
            datetime(2024, 3, 5),
            datetime(2024, 3, 20),
            datetime(2024, 3, 12),
            datetime(2024, 3, 8),
        ],
        'status': ['completed', 'completed', 'pending', 'completed', 'shipped', 'completed']
    }
    
    return customers_data, orders_data


def demo_basic_indexing():
    """Demo 1: Basic indexing without nested data."""
    print("\n" + "="*70)
    print("DEMO 1: Basic Indexing")
    print("="*70)
    
    # Create simple ibis table
    data = {
        'id': [1, 2, 3, 4, 5],
        'value': [10, 20, 30, 40, 50],
        'timestamp': pd.date_range('2024-01-01', periods=5)
    }
    
    # Create leanframe DataFrame
    ibis_table = ibis.memtable(data)
    df = DataFrame(ibis_table)
    
    print(f"\nOriginal DataFrame shape: {len(df.columns)} columns")
    print(f"Columns: {df.columns.tolist()}")
    
    # Set index on timestamp
    print("\n📍 Setting index on 'timestamp' (ascending)...")
    df_indexed = df.set_index('timestamp', ascending=True)
    print(f"Index: {df_indexed.index}")
    
    # Use iloc
    print("\n🔢 Using .iloc for position-based access:")
    print("\n  First 2 rows (df.iloc[0:2]):")
    first_2 = df_indexed.iloc[0:2]
    print(first_2.to_pandas())
    
    # Use head/tail
    print("\n📊 Using .head() and .tail():")
    print("\n  First 3 rows (df.head(3)):")
    print(df_indexed.head(3).to_pandas())
    
    print("\n  Last 2 rows (df.tail(2)):")
    print(df_indexed.tail(2).to_pandas())
    
    # Use loc
    print("\n🏷️  Setting index on 'id' for .loc access:")
    df_by_id = df.set_index('id')
    print("\n  Get row where id=3 (df.loc[3]):")
    print(df_by_id.loc[3].to_pandas())
    
    print("\n  Get range id=2:4 (df.loc[2:4]):")
    print(df_by_id.loc[2:4].to_pandas())


def demo_nested_data_with_indexing():
    """Demo 2: Indexing with nested data extraction."""
    print("\n" + "="*70)
    print("DEMO 2: Nested Data + Indexing")
    print("="*70)
    
    customers_data, _ = create_sample_nested_data()
    
    # Create leanframe DataFrame with nested data
    ibis_table = ibis.memtable(customers_data)
    customers_df = DataFrame(ibis_table)
    
    print(f"\nOriginal nested DataFrame:")
    print(f"Columns: {customers_df.columns.tolist()}")
    
    # Create handler to analyze nested structure
    print("\n🔍 Creating DataFrameHandler to analyze nested structure...")
    handler = DataFrameHandler(customers_df)
    handler.show_structure()
    
    # Extract nested fields
    print("\n📤 Extracting nested fields...")
    flat_df = handler.extract_nested_fields(verbose=False)
    print(f"Flattened columns: {flat_df.columns.tolist()}")
    
    # Now apply indexing to the flattened DataFrame
    print("\n📍 Setting index on 'profile_age' (descending - oldest first)...")
    by_age = flat_df.set_index('profile_age', ascending=False)
    
    print("\n👴 Oldest 3 customers (by_age.head(3)):")
    print(by_age.head(3).to_pandas()[['customer_id', 'profile_name', 'profile_age', 'profile_email']])
    
    print("\n👶 Youngest 2 customers (by_age.tail(2)):")
    print(by_age.tail(2).to_pandas()[['customer_id', 'profile_name', 'profile_age', 'profile_email']])
    
    # Index by registration date
    print("\n📍 Setting index on 'registration_date' (newest first)...")
    by_date = flat_df.set_index('registration_date', ascending=False)
    
    print("\n🆕 Most recent 3 registrations (by_date.iloc[0:3]):")
    print(by_date.iloc[0:3].to_pandas()[['customer_id', 'profile_name', 'registration_date']])
    
    # Use .loc on customer_id
    print("\n📍 Setting index on 'customer_id' for .loc access...")
    by_id = flat_df.set_index('customer_id')
    
    print("\n🎯 Get specific customers (by_id.loc[[1001, 1003]]):")
    print(by_id.loc[[1001, 1003]].to_pandas()[['customer_id', 'profile_name', 'profile_email']])


def demo_joins_with_indexing():
    """Demo 3: Combining NestedHandler joins with indexing."""
    print("\n" + "="*70)
    print("DEMO 3: Joins + Indexing")
    print("="*70)
    
    customers_data, orders_data = create_sample_nested_data()
    
    # Create DataFrames
    customers_ibis = ibis.memtable(customers_data)
    orders_ibis = ibis.memtable(orders_data)
    
    customers_df = DataFrame(customers_ibis)
    orders_df = DataFrame(orders_ibis)
    
    # Setup NestedHandler
    print("\n🔧 Setting up NestedHandler...")
    handler = NestedHandler()
    handler.add("customers", customers_df)
    handler.add("orders", orders_df)
    
    # Prepare customers (extract nested fields)
    print("\n📤 Preparing customers DataFrame (extracting nested fields)...")
    customers_prep = handler.prepare("customers", verbose=False)
    
    # Add prepared version back
    handler.add("customers_flat", customers_prep)
    
    # Perform join
    print("\n🔗 Joining customers with orders...")
    joined = handler.join(
        tables={"c": "customers_flat", "o": "orders"},
        on=[("c", "customer_id", "o", "customer_id")],
        how="inner"
    )
    
    print(f"Joined DataFrame columns: {len(joined.columns)}")
    
    # Apply indexing to joined result
    print("\n📍 Setting index on 'order_date' (most recent first)...")
    by_date = joined.set_index('order_date', ascending=False)
    
    print("\n🆕 Most recent 3 orders (by_date.head(3)):")
    result = by_date.head(3).to_pandas()
    print(result[['order_id', 'profile_name', 'amount', 'order_date', 'status']])
    
    # Index by amount
    print("\n📍 Setting index on 'amount' (highest first)...")
    by_amount = joined.set_index('amount', ascending=False)
    
    print("\n💰 Top 3 highest value orders (by_amount.iloc[0:3]):")
    result = by_amount.iloc[0:3].to_pandas()
    print(result[['order_id', 'profile_name', 'profile_email', 'amount', 'order_date']])
    
    # Use .loc to filter by customer_id
    print("\n📍 Setting index on 'customer_id' for .loc filtering...")
    by_customer = joined.set_index('customer_id')
    
    print("\n👤 All orders for customer 1001 (by_customer.loc[1001]):")
    result = by_customer.loc[1001].to_pandas()
    print(result[['order_id', 'profile_name', 'amount', 'order_date']])


def demo_chaining_operations():
    """Demo 4: Chaining indexing with other operations."""
    print("\n" + "="*70)
    print("DEMO 4: Chaining Operations")
    print("="*70)
    
    _, orders_data = create_sample_nested_data()
    
    orders_ibis = ibis.memtable(orders_data)
    orders_df = DataFrame(orders_ibis)
    
    print("\n📊 Original orders:")
    print(orders_df.to_pandas())
    
    # Chain: filter -> set index -> slice
    print("\n🔗 Chaining: filter completed orders, order by date, get top 2...")
    
    # Filter completed orders (using Ibis directly)
    completed = orders_df._data.filter(orders_df._data.status == 'completed')
    completed_df = DataFrame(completed)
    
    # Set index and slice
    by_date = completed_df.set_index('order_date', ascending=False)
    recent_completed = by_date.iloc[0:2]
    
    print("\n✅ Most recent 2 completed orders:")
    print(recent_completed.to_pandas())
    
    # Chain: order by amount, get top 3, then filter by customer
    print("\n🔗 Chaining: order by amount (desc), get top 3...")
    by_amount = orders_df.set_index('amount', ascending=False)
    top_3_value = by_amount.iloc[0:3]
    
    print("\n💎 Top 3 highest value orders:")
    print(top_3_value.to_pandas())


def demo_error_cases():
    """Demo 5: Common error cases and how to handle them."""
    print("\n" + "="*70)
    print("DEMO 5: Error Handling")
    print("="*70)
    
    data = {
        'id': [1, 2, 3],
        'value': [10, 20, 30]
    }
    ibis_table = ibis.memtable(data)
    df = DataFrame(ibis_table)
    
    # Error 1: Using .iloc without setting index
    print("\n❌ Error 1: Using .iloc without index...")
    try:
        df.iloc[0]
    except ValueError as e:
        print(f"Caught expected error: {e}")
    
    # Error 2: Using .loc without setting index
    print("\n❌ Error 2: Using .loc without index...")
    try:
        df.loc[1]
    except ValueError as e:
        print(f"Caught expected error: {e}")
    
    # Error 3: Setting index on non-existent column
    print("\n❌ Error 3: Setting index on non-existent column...")
    try:
        df.set_index('nonexistent')
    except KeyError as e:
        print(f"Caught expected error: {e}")
    
    # Success: Proper usage
    print("\n✅ Proper usage: set index first...")
    df_indexed = df.set_index('id')
    print(f"Index set: {df_indexed.index}")
    print("Now .iloc and .loc work correctly!")
    result = df_indexed.loc[2]
    print(result.to_pandas())


def demo_multi_column_ordering():
    """Demo 6: Multi-column composite ordering (like SQL ORDER BY col1, col2)."""
    print("\n" + "="*70)
    print("DEMO 6: Multi-Column Ordering")
    print("="*70)
    
    # Create sample data with priority and timestamp
    task_data = {
        'task_id': [101, 102, 103, 104, 105, 106, 107, 108],
        'priority': [1, 1, 2, 2, 3, 3, 1, 2],
        'timestamp': [
            datetime(2024, 3, 1, 10, 0),
            datetime(2024, 3, 1, 9, 0),
            datetime(2024, 3, 1, 11, 0),
            datetime(2024, 3, 1, 8, 0),
            datetime(2024, 3, 1, 12, 0),
            datetime(2024, 3, 1, 7, 0),
            datetime(2024, 3, 1, 13, 0),
            datetime(2024, 3, 1, 14, 0),
        ],
        'description': ['Task A', 'Task B', 'Task C', 'Task D', 'Task E', 'Task F', 'Task G', 'Task H']
    }
    
    ibis_table = ibis.memtable(task_data)
    df = DataFrame(ibis_table)
    
    print("\nOriginal data (unordered):")
    print(df.to_pandas()[['task_id', 'priority', 'timestamp', 'description']])
    
    # Single-column ordering
    print("\n📍 Single-column index on 'priority' (ascending):")
    by_priority = df.set_index('priority')
    print(by_priority.to_pandas()[['task_id', 'priority', 'timestamp', 'description']])
    print("Note: Within each priority level, order is not deterministic")
    
    # Multi-column ordering - priority ASC, timestamp ASC
    print("\n📍 Multi-column index: ['priority', 'timestamp'] (both ascending):")
    by_priority_time = df.set_index(['priority', 'timestamp'], ascending=True)
    print(by_priority_time.to_pandas()[['task_id', 'priority', 'timestamp', 'description']])
    print("Result: Ordered by priority, then by earliest timestamp within each priority")
    
    # Multi-column with different directions
    print("\n📍 Multi-column index: ['priority', 'timestamp'] with [DESC, ASC]:")
    by_priority_desc = df.set_index(['priority', 'timestamp'], ascending=[False, True])
    print(by_priority_desc.to_pandas()[['task_id', 'priority', 'timestamp', 'description']])
    print("Result: Highest priority first, then earliest timestamp (priority queue)")
    
    # Use with iloc
    print("\n🎯 Getting top 3 tasks with multi-column ordering:")
    print("   (Highest priority first, earliest timestamp breaks ties)")
    top_3 = by_priority_desc.iloc[0:3]
    print(top_3.to_pandas()[['task_id', 'priority', 'timestamp', 'description']])
    
    # Example with nested data
    print("\n📍 Multi-column ordering with nested fields:")
    customers_data, _ = create_sample_nested_data()
    customers_df = DataFrame(ibis.memtable(customers_data))
    handler = DataFrameHandler(customers_df)
    flat_df = handler.extract_nested_fields(verbose=False)
    
    # Order by age DESC, then registration_date ASC
    by_age_date = flat_df.set_index(['profile_age', 'registration_date'], ascending=[False, True])
    print("\n   Ordered by age DESC (nested field), registration_date ASC (regular field):")
    print(by_age_date.to_pandas()[['customer_id', 'profile_name', 'profile_age', 'registration_date']])
    print("\n   SQL equivalent: ORDER BY profile_age DESC, registration_date ASC")
    print("\n   This demonstrates ordering across different nesting levels:")
    print("   - 'profile_age' comes from nested 'profile' struct")
    print("   - 'registration_date' is a regular top-level column")
    
    # More complex example: order by regular column, then multiple nested fields
    print("\n📍 Complex multi-level ordering:")
    print("   Order by registration_date DESC (regular), then profile_age ASC (nested), then profile_name ASC (nested):")
    complex_order = flat_df.set_index(
        ['registration_date', 'profile_age', 'profile_name'],
        ascending=[False, True, True]
    )
    result = complex_order.to_pandas()[['customer_id', 'profile_name', 'profile_age', 'registration_date']]
    print(result)
    print("\n   This shows:")
    print("   - Primary sort: newest registrations first (regular column)")
    print("   - Secondary sort: youngest first within same date (nested field)")
    print("   - Tertiary sort: alphabetical by name for same age (nested field)")


if __name__ == "__main__":
    print("\n" + "🚀 LEANFRAME INDEXING EXAMPLES" + "\n")
    print("This demo shows indexing features with nested data support")
    
    # Run all demos
    demo_basic_indexing()
    demo_nested_data_with_indexing()
    demo_joins_with_indexing()
    demo_chaining_operations()
    demo_error_cases()
    demo_multi_column_ordering()
    
    print("\n" + "="*70)
    print("✅ All demos completed!")
    print("="*70)
    print("\nKey Takeaways:")
    print("1. Always set index explicitly for deterministic ordering")
    print("2. Use .iloc for position-based access (with ordering)")
    print("3. Use .loc for value-based filtering (on index column)")
    print("4. Indexing works seamlessly with nested data extraction")
    print("5. Chain operations: extract -> flatten -> index -> slice")
    print("6. Multi-column ordering: like SQL ORDER BY col1 DESC, col2 ASC")
    print("\nSee docs/indexing_guide.md for more details!")
