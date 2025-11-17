# NestedHandler Architecture - Design Vision

## Critical Design Principle

**NestedHandler prepares data. Ibis/Backend executes SQL.**

### Why This Matters

Your feedback was spot-on:
1. ❌ Multiple `join_on_*` methods are unnecessary complexity
2. ❌ Two-table limitation is artificial
3. ❌ Need support for ALL SQL features (WHERE, HAVING, windows, etc.)
4. ✅ Should delegate to Ibis (which already supports everything!)

## New Architecture

```
┌─────────────────────────────────────────────────────┐
│  NestedHandler: Data Preparation Layer              │
│                                                      │
│  Responsibilities:                                   │
│  • Analyze nested structure                         │
│  • Extract nested fields → flat columns             │
│  • Manage multiple DataFrames                       │
│  • Track backend references                         │
└──────────────────┬──────────────────────────────────┘
                   │
                   │ prepare() returns flat DataFrame
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│  Ibis/Backend: SQL Execution Layer                  │
│                                                      │
│  Full SQL Power:                                     │
│  • Joins: ANY number of tables, ALL types           │
│  • WHERE, HAVING clauses                            │
│  • GROUP BY + aggregations                          │
│  • Window functions                                 │
│  • UNION, INTERSECT, EXCEPT                         │
│  • CTEs (Common Table Expressions)                  │
│  • Everything BigQuery/SQL supports!                │
└─────────────────────────────────────────────────────┘
```

## Key Methods

### `prepare(name, fields=None, verbose=False)`
Extract nested fields from a DataFrame, returning a flattened version.

```python
# Extract all nested fields
customers_flat = handler.prepare("customers")

# Extract specific fields only
customers_flat = handler.prepare("customers", fields=["profile.email", "profile.name"])
```

### Direct Ibis Operations
After `prepare()`, use full Ibis/SQL power:

```python
# Simple join
result = table1._data.join(table2._data, predicates=[...], how="inner")

# Complex multi-table join
step1 = customers._data.join(orders._data, ...)
step2 = step1.join(products._data, ...)
step3 = step2.join(regions._data, ...)

# Add WHERE
filtered = step3.filter(step3.amount > 100)

# Add GROUP BY
grouped = filtered.group_by("region").aggregate(
    total=filtered.amount.sum(),
    count=filtered.order_id.count()
)

# Add HAVING
having_filtered = grouped.filter(grouped.total > 1000)

# Window functions
with_rank = grouped.select(
    grouped.region,
    grouped.total,
    rank=ibis.row_number().over(window)
)
```

## Usage Pattern

### 1. Add DataFrames
```python
handler = NestedHandler()
handler.add("customers", customers_df, table_qualifier="db.sales.customers")
handler.add("orders", orders_df, table_qualifier="db.sales.orders")
handler.add("products", products_df)
```

### 2. Prepare (Extract Nested Fields)
```python
# NestedHandler handles the complex part: nested field extraction
customers_flat = handler.prepare("customers")  # Extracts profile.email → profile_email
orders_flat = handler.prepare("orders")        # Extracts shipping.email → shipping_email
products_flat = handler.prepare("products")    # Already flat, returns as-is
```

### 3. Use Ibis for SQL Operations
```python
# Ibis handles the SQL part: all operations BigQuery supports
joined = customers_flat._data.join(
    orders_flat._data,
    predicates=[customers_flat._data.profile_email == orders_flat._data.shipping_email],
    how="inner"
).join(
    products_flat._data,
    predicates=[orders_flat._data.product_id == products_flat._data.product_id],
    how="left"
).filter(
    lambda t: t.amount > 100
).group_by("customer_id").aggregate(
    total=lambda t: t.amount.sum(),
    count=lambda t: t.order_id.count()
)

result = DataFrame(joined)
```

## Why This is Better

### ✅ Separation of Concerns
- **NestedHandler**: Knows about nested structures (domain-specific)
- **Ibis**: Knows about SQL operations (already mature, tested)

### ✅ No Artificial Limitations
- Join ANY number of tables
- Use ANY join type Ibis supports
- Add ANY SQL clause (WHERE, HAVING, etc.)
- Use window functions, CTEs, everything!

### ✅ Future-Proof
- When BigQuery adds new features, they work immediately (via Ibis)
- No need to wrap every SQL operation
- NestedHandler focuses on its unique value: nested data

### ✅ Composable
```python
# Each step is a standard DataFrame
customers_flat = handler.prepare("customers")
orders_flat = handler.prepare("orders")

# Combine however you want
result1 = customers_flat._data.join(orders_flat._data, ...)
result2 = customers_flat._data.filter(...)
result3 = result1.union(result2)
```

## Future: Convenience Method

Eventually we might add:
```python
# Convenience wrapper for common cases
result = handler.join(
    tables={"c": "customers", "o": "orders", "p": "products"},
    on=[("c", "email", "o", "customer_email"),
        ("o", "product_id", "p", "product_id")],
    how="inner"
)
```

**BUT** this is just sugar over `prepare()` + Ibis operations. Power users can always go direct!

## Migration Path

### Old Code (Deprecated)
```python
result = handler.join_on_both_nested(
    left="customers",
    right="orders",
    left_path="profile.email",
    right_path="shipping.email",
    how="inner"
)
```

### New Code (Flexible)
```python
# Prepare
customers = handler.prepare("customers")
orders = handler.prepare("orders")

# Join with full SQL power
result_ibis = customers._data.join(
    orders._data,
    predicates=[customers._data.profile_email == orders._data.shipping_email],
    how="inner"
)

# Add more operations as needed
filtered = result_ibis.filter(result_ibis.amount > 100)
grouped = filtered.group_by("customer_id").aggregate(...)

result = DataFrame(filtered)  # Or grouped, or any Ibis expression
```

## Implementation Status

- ✅ `prepare()` method implemented
- ✅ Backend reference management (property-based)
- ✅ Demo showing flexible joins
- ✅ Documentation
- ⏳ Convenience `join()` method (future)
- ❌ Old `join_on_*` methods deprecated (raise DeprecationWarning)

## Examples

See:
- `demos/demo_flexible_joins.py` - Comprehensive examples
- `docs/architecture/backend_reference_management.md` - Architecture details

## Summary

**The right tool for the right job:**
- NestedHandler: Expert at nested data preparation
- Ibis: Expert at SQL operations
- Together: Full BigQuery/pandas emulation with no limitations!
