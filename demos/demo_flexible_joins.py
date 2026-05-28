#!/usr/bin/env python3
"""
Demo: Flexible SQL-like Joins with NestedHandler

⚠️  NOTE: This demo has been CONSOLIDATED into demo_nested_handler_backend.py
    See that file for the LATEST and most comprehensive examples of:
    - APPROACH 1: Convenience join() method (regular users)
    - APPROACH 2: prepare() + Ibis (power users with full SQL)

This file is kept for reference showing additional advanced examples:
- Three-table joins
- Window functions
- Complex GROUP BY queries
- Different join types

For the main demo, see: demos/demo_nested_handler_backend.py

IMPORTANT: Column naming convention
- User-facing: Use dot notation for nested paths (e.g., "profile.contact.email")
- Internal: After prepare(), columns use underscores (e.g., "profile_contact_email")
- In this demo: We show the AFTER prepare() state, so you see underscore names

---

Original Description:
This demonstrates the NEW architecture where:
1. NestedHandler prepares DataFrames (extracts nested fields)
2. Ibis handles all SQL operations (joins, WHERE, HAVING, windows, etc.)
3. No artificial limitations - full SQL flexibility!

Key Benefits:
- Join ANY number of tables
- Use ANY join type (inner, left, right, outer, cross, semi, anti)
- Add WHERE clauses, HAVING, window functions
- Multi-column joins
- Complex predicates
- Everything SQL/BigQuery supports!
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import ibis
import pandas as pd
import leanframe
from leanframe.core.frame import DataFrame
from leanframe.core.nested_handler import NestedHandler
from demos.utils.create_nested_data import (
    create_customers_for_join,
    create_orders_for_join,
)


def main():
    print("=" * 70)
    print("🚀 Flexible SQL-like Joins with NestedHandler")
    print("=" * 70)

    # Setup
    backend = ibis.duckdb.connect()
    session = leanframe.Session(backend=backend)
    nested = NestedHandler()

    # ===================================================================
    # STEP 1: Prepare Data
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 1: Prepare Test Data")
    print("=" * 70)

    # Create nested DataFrames
    customers_df = create_customers_for_join()
    orders_df = create_orders_for_join()

    # Add third table - products
    products_pd = pd.DataFrame(
        {
            "product_id": [1, 2, 3],
            "name": ["Widget", "Gadget", "Doohickey"],
            "category": ["Electronics", "Electronics", "Hardware"],
            "price": [29.99, 149.99, 9.99],
        }
    )
    products_df = session.DataFrame(products_pd)

    # Add to NestedHandler
    nested.add("customers", session.DataFrame(customers_df.to_pandas()))
    nested.add("orders", session.DataFrame(orders_df.to_pandas()))
    nested.add("products", products_df)

    print("\n✅ Added 3 tables:")
    print("   - customers: nested profile.contact.email")
    print("   - orders: nested shipping.recipient.email")
    print("   - products: flat structure")

    # ===================================================================
    # STEP 2: Simple Two-Table Join
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 2: Simple Two-Table Join (NEW Approach)")
    print("=" * 70)

    print("\n📋 OLD way (limited, deprecated):")
    print("   result = nested.join_on_both_nested(...)")
    print("   ❌ Only 2 tables")
    print("   ❌ Limited join types")
    print("   ❌ No WHERE, HAVING, etc.")

    print("\n📋 NEW way (full SQL power):")
    print("   1. Prepare: Extract nested fields")
    print("   2. Join: Use Ibis directly")
    print("   3. Query: Add WHERE, GROUP BY, etc.")

    # Step 1: Prepare - extract nested fields
    print("\n1️⃣ Prepare DataFrames (extract nested fields):")
    customers_flat = nested.prepare("customers")
    orders_flat = nested.prepare("orders")

    print(f"   Customers: {len(customers_flat.columns)} columns")
    print(f"   Orders: {len(orders_flat.columns)} columns")

    # Step 2: Join using Ibis
    print("\n2️⃣ Join using Ibis (full SQL flexibility):")
    # Note: After prepare(), nested fields are flattened with underscores
    # e.g., "profile.contact.email" → "profile_contact_email" (column name)
    # For simple joins, you could use: nested.join(..., on=[("c", "profile.contact.email", ...)])
    joined = customers_flat._data.join(
        orders_flat._data,
        predicates=[
            customers_flat._data.profile_contact_email
            == orders_flat._data.shipping_recipient_email
        ],
        how="inner",
    )

    result = DataFrame(joined)
    print(
        f"   ✅ Joined: {len(result.columns)} columns, {joined.count().execute()} rows"
    )

    # Step 3: Add WHERE clause
    print("\n3️⃣ Add WHERE clause (filter results):")
    filtered = joined.filter(joined.amount > 100)  # type: ignore
    result_filtered = DataFrame(filtered)  # noqa
    print(f"   ✅ Filtered (amount > 100): {filtered.count().execute()} rows")

    # ===================================================================
    # STEP 3: Three-Table Join
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 3: Three-Table Join (IMPOSSIBLE with old methods!)")
    print("=" * 70)

    print("\n🎯 Goal: customers ⋈ orders ⋈ products")
    print("   Old methods: ❌ Can't do this!")
    print("   New approach: ✅ Easy!")

    # Modify orders to have product_id
    orders_with_products_pd = orders_flat.to_pandas()
    orders_with_products_pd["product_id"] = [1, 2, 1, 3, 2]  # Match order IDs
    orders_with_products = session.DataFrame(orders_with_products_pd)

    print("\n1️⃣ First join: customers ⋈ orders")
    step1 = customers_flat._data.join(
        orders_with_products._data,
        predicates=[
            customers_flat._data.profile_contact_email
            == orders_with_products._data.shipping_recipient_email
        ],
        how="inner",
    )

    print("\n2️⃣ Second join: (customers ⋈ orders) ⋈ products")
    step2 = step1.join(
        products_df._data,
        predicates=[step1.product_id == products_df._data.product_id],
        how="inner",
    )

    three_table_result = DataFrame(step2)
    print(f"   ✅ Three-table join: {len(three_table_result.columns)} columns")
    print(f"   ✅ Result: {step2.count().execute()} rows")

    # ===================================================================
    # STEP 4: Complex Query with GROUP BY
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 4: Complex Query (GROUP BY + Aggregation)")
    print("=" * 70)

    print("\n🎯 Calculate total sales per customer")

    # Group and aggregate
    grouped = step2.group_by("customer_id").aggregate(
        total_amount=step2.amount.sum(),  # type: ignore
        order_count=step2.order_id.count(),  # type: ignore
        avg_price=step2.price.mean(),  # type: ignore
    )

    grouped_result = DataFrame(grouped)
    print(f"   ✅ Grouped by customer: {grouped.count().execute()} customers")
    print("\n   Sample results:")
    sample = grouped_result.to_pandas().head(3)
    for _, row in sample.iterrows():
        print(
            f"      Customer {row['customer_id']}: "
            f"{row['order_count']} orders, "
            f"${row['total_amount']:.2f} total, "
            f"${row['avg_price']:.2f} avg"
        )

    # ===================================================================
    # STEP 5: Different Join Types
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 5: Different Join Types (ALL supported!)")
    print("=" * 70)

    print("\n✅ Supported join types:")
    join_types = ["inner", "left", "right", "outer", "cross", "semi", "anti"]
    for jt in join_types:
        print(f"   • {jt}")

    print("\n📝 Example: LEFT JOIN (keep all customers)")
    left_join = customers_flat._data.join(
        orders_flat._data,
        predicates=[
            customers_flat._data.profile_contact_email
            == orders_flat._data.shipping_recipient_email
        ],
        how="left",
    )
    left_result = DataFrame(left_join)
    print(
        f"   ✅ LEFT JOIN: {left_result._data.count().execute()} rows (includes customers without orders)"
    )

    # ===================================================================
    # STEP 6: Window Functions
    # ===================================================================
    print("\n" + "=" * 70)
    print("STEP 6: Window Functions (BigQuery/SQL feature)")
    print("=" * 70)

    print("\n🎯 Add row numbers within each customer's orders")

    # Use Ibis window functions
    window = ibis.window(group_by=step2.customer_id, order_by=step2.amount.desc())
    with_rank = step2.select(
        step2.customer_id,
        step2.order_id,
        step2.amount,
        order_rank=ibis.row_number().over(window),
    )

    rank_result = DataFrame(with_rank)
    print(f"   ✅ Added ranking: {rank_result._data.count().execute()} rows")
    print("\n   Sample with rankings:")
    sample = rank_result.to_pandas().head(5)
    for _, row in sample.iterrows():
        print(
            f"      Customer {row['customer_id']}, "
            f"Order {row['order_id']}: "
            f"${row['amount']:.2f} (rank #{row['order_rank']})"
        )

    # ===================================================================
    # Summary
    # ===================================================================
    print("\n" + "=" * 70)
    print("✅ Summary: Why This Architecture is Better")
    print("=" * 70)

    print("\n🎯 Key Advantages:")
    print("   1. ✅ Join ANY number of tables (not just 2)")
    print("   2. ✅ ALL join types: inner, left, right, outer, cross, semi, anti")
    print("   3. ✅ Multi-column joins")
    print("   4. ✅ Complex predicates (AND, OR, etc.)")
    print("   5. ✅ WHERE clauses")
    print("   6. ✅ GROUP BY + aggregations (SUM, COUNT, AVG, etc.)")
    print("   7. ✅ HAVING clauses")
    print("   8. ✅ Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)")
    print("   9. ✅ UNION, INTERSECT, EXCEPT")
    print("   10. ✅ Everything BigQuery/SQL supports!")

    print("\n📐 Design Pattern:")
    print("   ┌─────────────────────────────────────────────┐")
    print("   │  NestedHandler: Prepare nested data         │")
    print("   │  (Extract nested fields → flat columns)     │")
    print("   └─────────────┬───────────────────────────────┘")
    print("                 │")
    print("                 ▼")
    print("   ┌─────────────────────────────────────────────┐")
    print("   │  Ibis/Backend: Handle ALL SQL operations    │")
    print("   │  (Joins, WHERE, GROUP BY, windows, etc.)    │")
    print("   └─────────────────────────────────────────────┘")

    print("\n🔧 Simple Workflow:")
    print("   1. handler.prepare('table_name')  # Extract nested → flat")
    print("   2. Use Ibis operations directly   # Full SQL power")
    print("   3. Result is ready for BigQuery   # Or any backend")

    print("\n💡 Future: handler.join() convenience method")
    print("   Will wrap this pattern for common cases")
    print("   But you ALWAYS have direct Ibis access for complex queries!")

    backend.disconnect()
    print("\n🔌 Disconnected from database")


if __name__ == "__main__":
    main()
