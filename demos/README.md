# LeanFrame Demos

This directory contains demonstration scripts for LeanFrame's capabilities.

## 📚 Main Demos (Start Here!)

### 🎯 **demo_nested_handler_backend.py** - COMPREHENSIVE DEMO ⭐

**The definitive guide to NestedHandler with backend integration.**

This demo shows TWO approaches for different user needs:

**APPROACH 1: Convenience `join()` Method (Regular Users)**
- ✅ Simple, intuitive API
- ✅ Automatic nested field extraction
- ✅ Multi-table, multi-column joins
- ✅ All join types supported (inner, left, right, outer, cross)
- ✅ No need to understand Ibis internals

**APPROACH 2: `prepare()` + Direct Ibis (Power Users)**
- ⚡ Maximum flexibility
- ⚡ Full control over SQL operations
- ⚡ WHERE, HAVING, window functions
- ⚡ Complex multi-table queries
- ⚡ Everything BigQuery/SQL supports

**Also covers:**
- Backend integration (DuckDB)
- Table push/pull operations
- Backend reference management
- DataFrame lifecycle (in-memory ↔ backend)
- Join provenance tracking

**Run it:**
```bash
python demos/demo_nested_handler_backend.py
```

---

## 📖 Additional Examples

### demo_flexible_joins.py
**Status:** Consolidated into `demo_nested_handler_backend.py`

Contains additional advanced examples:
- Three-table joins
- Window functions (ROW_NUMBER, RANK, etc.)
- Complex GROUP BY queries
- Various join types

**Note:** For most use cases, see `demo_nested_handler_backend.py` instead.

### demo_dynamic_nested_handler.py
Shows dynamic creation and manipulation of nested DataFrames.

---

## 🚀 Quick Start Guide

### For Regular Users (Simple Joins)
```python
from leanframe.core.nested_handler import NestedHandler

# Create handler
handler = NestedHandler()
handler.add("customers", customers_df)
handler.add("orders", orders_df)

# Simple join - handles nested fields automatically
# Use natural dot notation for nested paths (converted internally to underscores)
result = handler.join(
    tables={"c": "customers", "o": "orders"},
    on=[("c", "profile.contact.email", "o", "shipping.recipient.email")],
    how="inner"
)
```

### For Power Users (Complex SQL)
```python
# Extract nested fields first
customers_flat = handler.prepare("customers")
orders_flat = handler.prepare("orders")

# Use direct Ibis for full SQL power
joined = customers_flat._data.join(
    orders_flat._data,
    predicates=[customers_flat._data.email == orders_flat._data.customer_email],
    how="inner"
)

# Add WHERE clause
filtered = joined.filter(joined.amount > 100)  # type: ignore

# Add GROUP BY
summary = filtered.group_by("customer_id").aggregate(
    total=filtered.amount.sum(),  # type: ignore
    count=filtered.order_id.count()  # type: ignore
)

from leanframe.core.frame import DataFrame
result = DataFrame(summary)
```

---

## 🧪 Testing

All features demonstrated here are covered by the test suite:
- `tests/unit/nested_data/test_prepare_method.py` - Tests for `prepare()`
- `tests/unit/nested_data/test_join_convenience.py` - Tests for `join()`
- Other nested data tests for core functionality

---

## 📝 Documentation

For more details on the architecture and design decisions, see:
- `NESTED_JOINS_STRATEGY.md` - Join implementation strategy
- API documentation (coming soon)

---

## ❓ Questions?

**Which demo should I run?**
→ Start with `demo_nested_handler_backend.py` - it's comprehensive!

**I just want simple joins, what do I use?**
→ Use the `join()` convenience method (APPROACH 1)

**I need complex SQL queries, what do I use?**
→ Use `prepare()` + direct Ibis (APPROACH 2)

**Can I mix both approaches?**
→ Yes! Use `join()` for simple cases, `prepare()` for complex ones.

**Where are the tests?**
→ See `tests/unit/nested_data/` for comprehensive test coverage.
