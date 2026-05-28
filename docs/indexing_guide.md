# Indexing in Leanframe

This guide explains how indexing works in leanframe and how it differs from pandas.

## Core Concept: Explicit Ordering

Unlike pandas where row order is intrinsic, SQL databases (like BigQuery) have no persistent row ordering. Leanframe makes ordering **explicit** through the index specification.

```python
# ❌ Pandas: assumes intrinsic row order
df.iloc[0:10]  # First 10 rows (but which rows?)

# ✅ Leanframe: explicit ordering
df.set_index('timestamp', ascending=False).iloc[0:10]  # 10 newest records
```

## Setting an Index

Use `.set_index()` to establish ordering:

```python
# Single column - ascending order
df = df.set_index('timestamp')

# Single column - descending order (newest first)
df = df.set_index('timestamp', ascending=False)

# Multi-column ordering (SQL: ORDER BY col1, col2)
df = df.set_index(['priority', 'timestamp'], ascending=[False, True])

# Multi-column with same direction for all
df = df.set_index(['region', 'customer_id'], ascending=True)
```

**Important:** The index doesn't create a new column or modify data. It's a **specification** for how to order rows in subsequent operations.

### Multi-Column Ordering

When you specify multiple columns, leanframe creates a composite ordering like SQL's `ORDER BY` clause:

```python
# Order by priority DESC, then timestamp ASC (for ties)
df = df.set_index(['priority', 'timestamp'], ascending=[False, True])

# Equivalent SQL:
# ORDER BY priority DESC, timestamp ASC
```

This is particularly useful for:
- **Breaking ties**: Primary sort by one column, secondary by another
- **Hierarchical data**: Sort by category, then subcategory, then item
- **Time series with groups**: Sort by group DESC, then timestamp ASC

**Examples:**

```python
# Customer data: group by region, then by signup date
df.set_index(['region', 'signup_date'], ascending=[True, False])

# Priority queue: highest priority first, earliest timestamp breaks ties
df.set_index(['priority', 'timestamp'], ascending=[False, True])

# Sales data: year DESC, quarter DESC, region ASC
df.set_index(['year', 'quarter', 'region'], ascending=[False, False, True])
```

You can specify a single `ascending` value to apply to all columns:

```python
# All ascending
df.set_index(['col1', 'col2', 'col3'], ascending=True)

# All descending
df.set_index(['col1', 'col2', 'col3'], ascending=False)
```

## Position-Based Indexing (.iloc)

Once an index is set, use `.iloc` for position-based access:

```python
# Set ordering first
df = df.set_index('timestamp', ascending=False)

# Single row
first = df.iloc[0]  # Newest record

# Slice
top_10 = df.iloc[0:10]  # 10 newest records
next_10 = df.iloc[10:20]  # Records 11-20

# Open-ended slices
from_10th = df.iloc[10:]  # All records from 10th onward
```

**SQL Translation:**
```python
df.iloc[10:20]
# Translates to:
# SELECT * FROM table ORDER BY timestamp DESC LIMIT 10 OFFSET 10
```

### Limitations

- No step size: `df.iloc[::2]` not supported (SQL doesn't support stepping)
- Negative indices expensive: `df.iloc[-1]` requires full table scan
- List indexing not yet implemented: `df.iloc[[1, 3, 5]]`

## Label-Based Indexing (.loc)

Use `.loc` to filter by index column values:

```python
# Set index on customer_id
df = df.set_index('customer_id')

# Single value
customer = df.loc[12345]  # WHERE customer_id = 12345

# Range
customers = df.loc[10000:20000]  # WHERE customer_id BETWEEN 10000 AND 20000

# Multiple values
customers = df.loc[[12345, 67890, 11111]]  # WHERE customer_id IN (...)
```

**SQL Translation:**
```python
df.loc[10000:20000]
# Translates to:
# SELECT * FROM table 
# WHERE customer_id >= 10000 AND customer_id <= 20000
# ORDER BY customer_id
```

## Convenience Methods

### .head() and .tail()

```python
# First 10 rows (requires index for deterministic results)
df.set_index('timestamp').head(10)

# Last 10 rows (requires index)
df.set_index('timestamp').tail(10)

# Default is 5 rows
df.head()
```

## Working with Nested Columns

Indexing works seamlessly with nested column handling:

```python
from leanframe.core.frame import DataFrameHandler

# Create handler for nested DataFrame
handler = DataFrameHandler(nested_df)

# Extract nested fields
flat_df = handler.extract_nested_fields()

# Now apply indexing
flat_df = flat_df.set_index('person_age')
oldest_10 = flat_df.iloc[0:10]
```

**Example with joins:**

```python
from leanframe.core.nested_handler import NestedHandler

# Setup
handler = NestedHandler()
handler.add("customers", customers_df)
handler.add("orders", orders_df)

# Prepare and join
joined = handler.join(
    tables={"c": "customers", "o": "orders"},
    on=[("c", "customer_id", "o", "customer_id")]
)

# Apply indexing to result
joined = joined.set_index('order_date', ascending=False)
recent_orders = joined.iloc[0:100]
```

## Best Practices

### 1. Always Set Index for Position-Based Operations

```python
# ❌ Bad: no explicit ordering
df.iloc[0:10]  # Raises ValueError

# ✅ Good: explicit ordering
df.set_index('id').iloc[0:10]
```

### 2. Choose Appropriate Index Columns

Good index columns:
- ✅ Timestamps (for time-series data)
- ✅ Sequential IDs (for deterministic ordering)
- ✅ Monotonic values

Avoid:
- ❌ Columns with many duplicates (unless that's intentional)
- ❌ Non-sortable types (complex nested structures)

### 3. Use .loc for Value-Based Filtering

```python
# ❌ Less efficient: filter then get first row
df.filter(df['customer_id'] == 12345).iloc[0]

# ✅ More direct: use .loc
df.set_index('customer_id').loc[12345]
```

### 4. Consider BigQuery Performance

```python
# Expensive: negative indexing requires full scan
df.iloc[-10:]  # Counts all rows to get last 10

# Better: use descending index + positive slice
df.set_index('timestamp', ascending=False).iloc[0:10]
```

## Comparison with Pandas

| Operation | Pandas | Leanframe |
|-----------|--------|-----------|
| Position access | `df.iloc[0]` | `df.set_index('col').iloc[0]` |
| Label access | `df.loc['label']` | `df.set_index('col').loc[value]` |
| Multi-index | Supported | **Not supported** |
| Negative indices | Fast | Slow (full scan) |
| Step slicing | `df.iloc[::2]` | **Not supported** |
| Index name | `df.index.name` | `df.index.name` |

## Design Philosophy

Leanframe's indexing design reflects SQL thinking:

1. **No Hidden State:** Ordering is explicit, not implicit
2. **SQL-First:** Operations map cleanly to SQL (LIMIT, OFFSET, WHERE)
3. **Performance Aware:** Design discourages expensive operations
4. **Simplicity:** No multi-index complexity

This makes leanframe code more **portable** to SQL and **predictable** in performance.

## Migration from Pandas

### Example: Time-Series Analysis

**Pandas:**
```python
# Assumes data is already sorted
df.iloc[0:100]  # First 100 rows
df.iloc[-100:]  # Last 100 rows
```

**Leanframe:**
```python
# Make sorting explicit
df = df.set_index('timestamp', ascending=True)
df.iloc[0:100]  # First 100 chronologically
df.set_index('timestamp', ascending=False).iloc[0:100]  # Most recent 100
```

### Example: Filtering by ID

**Pandas:**
```python
df.set_index('customer_id')
customer = df.loc[12345]
```

**Leanframe:**
```python
# Same syntax!
df.set_index('customer_id')
customer = df.loc[12345]
```

## Advanced: Custom Ordering Logic

For complex ordering (multiple columns, null handling), directly use Ibis:

```python
# Complex ordering with Ibis
ordered = df._data.order_by([
    ibis.desc(df._data.priority),
    df._data.timestamp
])

from leanframe.core.frame import DataFrame
df_ordered = DataFrame(ordered)

# Now use indexing
df_ordered._index = Index('priority', ascending=False)
top_priority = df_ordered.iloc[0:10]
```

## Future Enhancements

Planned features (not yet implemented):

- [ ] List-based .iloc indexing: `df.iloc[[1, 3, 5]]`
- [ ] Boolean indexing: `df.loc[df['amount'] > 100]`
- [ ] Multi-column ordering (composite index)
- [ ] Index persistence across operations
- [ ] Reset index: `df.reset_index()`
- [ ] Set index from expression: `df.set_index(df['col1'] + df['col2'])`

## See Also

- [DataFrame Methods Spec](../specs/2025-10-27-dataframe-methods.md)
- [Nested Handler Vision](architecture/NESTED_HANDLER_VISION.md)
- [Ibis Documentation](https://ibis-project.org/)
