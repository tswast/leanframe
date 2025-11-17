#!/usr/bin/env python3
"""
Comprehensive Demo: NestedHandler with Backend Integration

This demo shows TWO approaches for working with nested data:

APPROACH 1: Convenience join() method (Regular Users)
- Simple, intuitive API for common join cases
- Automatic nested field extraction
- Multi-table joins with clean syntax

APPROACH 2: prepare() + Direct Ibis (Power Users)  
- Maximum flexibility for complex queries
- WHERE, HAVING, window functions, CTEs
- Full SQL power when you need it

Also demonstrates:
- Backend integration (load/save from DuckDB)
- Backend reference management (table qualifiers)
- Lineage tracking
- In-memory → backend → in-memory lifecycle

Key Architectural Principles:
- One NestedHandler = One Backend Connection
- DataFrameHandler owns its backend reference (table_qualifier property)
- NestedHandler prepares data, Ibis executes SQL
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import ibis
import leanframe
from leanframe.core.nested_handler import NestedHandler
from demos.utils.create_nested_data import (
    create_customers_for_join,
    create_orders_for_join
)


def main():
    print("=" * 70)
    print("🚀 Comprehensive NestedHandler Demo with Backend Integration")
    print("=" * 70)
    
    # ===================================================================
    # STEP 1-2: Data Preparation - Create DataFrames and Push to DB
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 1-2: Data Preparation")
    print("=" * 70)
    
    print("\n📊 Creating nested DataFrames...")
    customers_df = create_customers_for_join()
    orders_df = create_orders_for_join()
    
    print("✅ Created two DataFrames:")
    print(f"   - Customers: email at profile.contact.email (nested 2 levels)")
    print(f"   - Orders: email at shipping.recipient.email (nested 2 levels, different path!)")
    
    # Connect to local DuckDB
    print("\n🔌 Connecting to local DuckDB...")
    backend = ibis.duckdb.connect()
    session = leanframe.Session(backend=backend)
    
    # Push to database
    print("\n💾 Pushing tables to local database...")
    
    # Convert to pandas temporarily for database insertion
    customers_pandas = customers_df.to_pandas()
    orders_pandas = orders_df.to_pandas()
    
    # Register tables in DuckDB
    backend.create_table("customers", customers_pandas, overwrite=True)
    backend.create_table("orders", orders_pandas, overwrite=True)
    
    print("✅ Tables created in database:")
    print("   - customers (4 records)")
    print("   - orders (5 records)")
    
    # ===================================================================
    # STEP 3: Create NestedHandler
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 3: Create NestedHandler Orchestrator")
    print("=" * 70)
    
    nested = NestedHandler()
    print("✅ Created NestedHandler instance")
    print(f"   Current state: {nested}")
    
    # ===================================================================
    # STEP 4-5: Load from DB and Add to Handler
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 4-5: Load Tables from Database → DataFrameHandlers → NestedHandler")
    print("=" * 70)
    
    print("\n📥 Loading 'customers' table from database...")
    customers_table = backend.table("customers")
    customers_lf = session.DataFrame(customers_table.to_pandas())
    
    # Add to NestedHandler with table qualifier for lineage
    nested.add(
        name="customers",
        df=customers_lf,
        table_qualifier="local_duckdb.main.customers"  # Full qualifier
    )
    
    print("\n📥 Loading 'orders' table from database...")
    orders_table = backend.table("orders")
    orders_lf = session.DataFrame(orders_table.to_pandas())
    
    nested.add(
        name="orders",
        df=orders_lf,
        table_qualifier="local_duckdb.main.orders"
    )
    
    print(f"\n✅ NestedHandler now manages: {nested}")
    
    # ===================================================================
    # STEP 5A: APPROACH 1 - Convenience join() Method (Regular Users)
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 5A: APPROACH 1 - Convenience join() Method")
    print("=" * 70)
    
    print("\n🎯 For Regular Users: Simple, intuitive API")
    print("   ✅ Automatic nested field extraction")
    print("   ✅ Clean multi-table join syntax")
    print("   ✅ No need to know about prepare() or Ibis")
    
    print("\n🔗 Joining customers and orders on email...")
    print("   - Customers: profile.contact.email (nested)")
    print("   - Orders: shipping.recipient.email (nested)")
    
    # Use convenience join() method - handles nested extraction automatically!
    joined_df = nested.join(
        tables={"c": "customers", "o": "orders"},
        on=[("c", "profile_contact_email", "o", "shipping_recipient_email")],
        how="inner"
    )
    
    print(f"\n✅ Join complete!")
    print(f"   Result DataFrame has {len(joined_df.columns)} columns")
    
    # Add result to handler for tracking
    nested.add("customer_orders_simple", joined_df, 
               table_qualifier="joined(local_duckdb.main.customers⋈local_duckdb.main.orders)")
    print(f"   Added to handler: 'customer_orders_simple'")
    print(f"   NestedHandler now has: {len(nested)} DataFrames")
    
    # ===================================================================
    # STEP 5B: APPROACH 2 - prepare() + Ibis (Power Users)
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 5B: APPROACH 2 - prepare() + Direct Ibis")
    print("=" * 70)
    
    print("\n⚡ For Power Users: Maximum flexibility")
    print("   ✅ Full control over SQL operations")
    print("   ✅ WHERE, HAVING, window functions")
    print("   ✅ Complex multi-table queries")
    
    print("\n🔧 Step 1: Prepare DataFrames (extract nested fields)")
    customers_flat = nested.prepare("customers", verbose=False)
    orders_flat = nested.prepare("orders", verbose=False)
    print(f"   Customers prepared: {len(customers_flat.columns)} columns")
    print(f"   Orders prepared: {len(orders_flat.columns)} columns")
    
    print("\n🔧 Step 2: Use direct Ibis operations")
    # Now we have full Ibis power!
    c_ibis = customers_flat._data
    o_ibis = orders_flat._data
    
    # Join on email
    joined_ibis = c_ibis.join(
        o_ibis,
        predicates=[c_ibis.profile_contact_email == o_ibis.shipping_recipient_email],
        how="inner"
    )
    
    # Can add WHERE clause (filter high-value orders)
    print("   Adding WHERE clause: amount > 100")
    # Note: Ibis operations - type checker may show warnings but code works
    filtered_ibis = joined_ibis.filter(joined_ibis.amount > 100)  # type: ignore
    
    # Can add GROUP BY and aggregations
    print("   Adding GROUP BY: customer_id")
    grouped_ibis = filtered_ibis.group_by("customer_id").aggregate(
        total_amount=filtered_ibis.amount.sum(),  # type: ignore
        order_count=filtered_ibis.order_id.count()  # type: ignore
    )
    
    from leanframe.core.frame import DataFrame as LeanDF
    power_user_result = LeanDF(grouped_ibis)
    
    print(f"\n✅ Power user query complete!")
    print(f"   Result has {len(power_user_result.columns)} columns")
    print(f"   With WHERE, GROUP BY, and aggregations")
    
    # Add to handler
    nested.add("customer_summary", power_user_result,
               table_qualifier="aggregated(customer_orders)")
    print(f"   Added to handler: 'customer_summary'")
    print(f"   NestedHandler now has: {len(nested)} DataFrames")
    
    # ===================================================================
    # STEP 6: Store Results in Database
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 6: Store Results Back to Database")
    print("=" * 70)
    
    # Store the simple join result
    print("\n💾 Writing convenience join result to database...")
    simple_result_handler = nested.get("customer_orders_simple")
    simple_pandas = simple_result_handler.original_df.to_pandas()
    backend.create_table("customer_orders_simple", simple_pandas, overwrite=True)
    print("✅ Created table: customer_orders_simple")
    print(f"   Rows: {len(simple_pandas)}")
    
    # Store the power user result
    print("\n� Writing power user summary to database...")
    summary_pandas = power_user_result.to_pandas()
    backend.create_table("customer_summary", summary_pandas, overwrite=True)
    print("✅ Created table: customer_summary")
    print(f"   Rows: {len(summary_pandas)}")
    
    # Show comparison
    print("\n📊 Results comparison:")
    print(f"   Simple join: {len(simple_pandas)} rows, {len(simple_pandas.columns)} columns")
    print(f"   Power user: {len(summary_pandas)} rows, {len(summary_pandas.columns)} columns (aggregated)")
    
    # Show sample data from simple join
    print("\n📋 Sample from simple join (first 2 records):")
    sample = simple_pandas.head(2)
    for i, (idx, row) in enumerate(sample.iterrows(), 1):
        print(f"\n   Record {i}:")
        print(f"      Customer ID: {row.get('customer_id', 'N/A')}")
        print(f"      Customer Name: {row.get('profile_name', 'N/A')}")
        print(f"      Email: {row.get('profile_contact_email', 'N/A')}")
        print(f"      Order ID: {row.get('order_id', 'N/A')}")
        print(f"      Order Amount: ${row.get('amount', 0):.2f}")
    
    # ===================================================================
    # STEP 7: Backend Reference Management - NEW Architecture!
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 7: Backend Reference Management (Property-Based)")
    print("=" * 70)
    
    print("\n📚 NEW Storage Architecture:")
    print("   Each DataFrameHandler now OWNS its backend reference!")
    print()
    print("   Old way (NestedHandler managed references):")
    print("      ❌ Tight coupling")
    print("      ❌ DataFrame can't update its own backend status")
    print()
    print("   NEW way (DataFrameHandler manages its own reference):")
    print("      ✅ Each handler has table_qualifier property")
    print("      ✅ Can be None (in-memory) or backend identifier")
    print("      ✅ Independent backend status updates")
    print("      ✅ Clean separation of concerns")
    
    print("\n🔍 Current backend status:")
    nested.show_backend_status()
    
    print("\n📋 Detailed handler information:")
    for df_name in nested.list_dataframes():
        handler = nested.get(df_name)
        backend_info = handler.get_backend_info()
        print(f"\n   {df_name}:")
        print(f"      {handler}")
        print(f"      Has backend table: {handler.has_backend_table()}")
        if handler.has_backend_table():
            print(f"      Qualifier: {handler.table_qualifier}")
            print(f"      Parsed: {backend_info}")
    
    # ===================================================================
    # STEP 8: Demonstrate Backend Reference Updates
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 8: Backend Reference Updates (NEW Feature)")
    print("=" * 70)
    
    print("\n💡 Demonstrating independent backend reference updates...")
    
    # Create an in-memory DataFrame
    print("\n1️⃣ Create in-memory DataFrame (no backend):")
    import pandas as pd
    temp_df = session.DataFrame(pd.DataFrame({
        'id': [1, 2, 3],
        'value': [10, 20, 30]
    }))
    nested.add("temp_data", temp_df)  # No table_qualifier
    
    temp_handler = nested.get("temp_data")
    print(f"   Status: {temp_handler.has_backend_table()} (no backend table)")
    
    # Simulate saving to backend
    print("\n2️⃣ Save to backend database:")
    temp_pandas = temp_df.to_pandas()
    backend.create_table("temp_data", temp_pandas, overwrite=True)
    
    # Update the backend reference
    temp_handler.set_table_qualifier("local_duckdb.main.temp_data")
    print(f"   Status: {temp_handler.has_backend_table()} (now has backend table!)")
    
    # Show updated status
    print("\n3️⃣ Show all backend status after update:")
    nested.show_backend_status()
    
    # Simulate dropping from backend
    print("\n4️⃣ Drop from backend (DataFrame still in memory):")
    backend.drop_table("temp_data")
    temp_handler.set_table_qualifier(None)  # Clear backend reference
    print(f"   Status: {temp_handler.has_backend_table()} (back to in-memory)")
    
    print("\n5️⃣ Final backend status:")
    nested.show_backend_status()
    
    print("\n✨ Benefits of NEW Architecture:")
    print("   ✅ Independent backend reference management")
    print("   ✅ DataFrames can update their own status")
    print("   ✅ Clean separation: NestedHandler orchestrates, handlers manage state")
    print("   ✅ Support for in-memory → backend → in-memory lifecycle")
    print("   ✅ Join provenance tracking (lineage from source tables)")
    print("   ✅ No tight coupling between orchestrator and backend state")
    
    # ===================================================================
    # Summary
    # ===================================================================
    print("\n" + "=" * 70)
    print("✅ Demo Complete!")
    print("=" * 70)
    
    print("\n📊 Summary:")
    print(f"   - Created 2 nested DataFrames with email in different nested paths")
    print(f"   - Pushed to local DuckDB: customers, orders")
    print(f"   - Loaded from DB into NestedHandler (with table qualifiers)")
    print(f"   - Joined on nested email: profile.contact.email ⋈ shipping.recipient.email")
    print(f"   - Result auto-added to handler: 'customer_orders'")
    print(f"   - Saved joined result back to database")
    print(f"   - Demonstrated backend reference updates (save/drop lifecycle)")
    
    print("\n🎯 Key Architectural Insights:")
    print("   1. DataFrameHandler owns its backend reference (table_qualifier property)")
    print("   2. NestedHandler orchestrates operations, handlers manage state")
    print("   3. Backend references can be None (in-memory) or qualified names")
    print("   4. Handlers can independently update backend status via set_table_qualifier()")
    print("   5. Clean lifecycle: in-memory → save (set qualifier) → drop (clear qualifier)")
    print("   6. Join results track lineage: joined(table1⋈table2)")
    print("   7. Auto-add results enable fluent chaining of operations")
    
    # Cleanup
    backend.disconnect()
    print("\n🔌 Disconnected from database")


if __name__ == "__main__":
    main()
