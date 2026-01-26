# Indexing Quick Reference

## Overview

Leanframe provides pandas-like indexing with SQL semantics. Since BigQuery and other SQL databases don't have persistent row ordering, indexing requires **explicit ORDER BY specification**.

## Key Concept

```python
# ❌ Pandas: implicit row order
df.iloc[0:10]

# ✅ Leanframe: explicit order specification
df.set_index('timestamp', ascending=False).iloc[0:10]
```

## API Reference

### Setting an Index

#### `DataFrame.set_index(columns, ascending=True, name=None)`

Establishes deterministic row ordering for position-based operations.

**Parameters:**
- `columns` (str or list[str]): Column name(s) to order by
- `ascending` (bool or list[bool]): Sort direction(s) (True=ASC, False=DESC)
- `name` (str, optional): Name for the index (defaults to first column name)

**Returns:** New DataFrame with index set

**Example:**
```python
# Single column - ascending order
df = df.set_index('customer_id')

# Single column - descending order
df = df.set_index('timestamp', ascending=False)

# Multi-column ordering (like SQL ORDER BY col1 DESC, col2 ASC)
df = df.set_index(['priority', 'timestamp'], ascending=[False, True])

# Multi-column with single ascending for all
df = df.set_index(['region', 'customer_id'], ascending=True)

# With custom name
df = df.set_index('score', ascending=False, name='rank')
```

**Multi-Column Ordering:**
When multiple columns are specified, they define a composite ordering just like SQL's `ORDER BY col1, col2, col3`. The first column is the primary sort, with subsequent columns breaking ties:

```python
# SQL equivalent: ORDER BY priority DESC, timestamp ASC, id ASC
df.set_index(['priority', 'timestamp', 'id'], ascending=[False, True, True])
```

---

### Position-Based Indexing (.iloc)

#### `DataFrame.iloc[key]`

Access rows by position using explicit ordering.

**Requires:** Index must be set first

**Supported keys:**
- `int`: Single row by position
- `slice`: Range of rows (e.g., `0:10`, `5:`, `:20`)

**Returns:** DataFrame (or Series for single row)

**Example:**
```python
df = df.set_index('timestamp', ascending=False)

# Single row
first = df.iloc[0]

# Slice
top_10 = df.iloc[0:10]
next_10 = df.iloc[10:20]
from_50 = df.iloc[50:]
```

**SQL Translation:**
```python
df.iloc[10:20]
# → SELECT * FROM table ORDER BY timestamp DESC LIMIT 10 OFFSET 10
```

**Limitations:**
- ❌ Step size not supported: `df.iloc[::2]`
- ⚠️ Negative indices expensive: `df.iloc[-1]` (requires full scan)
- ❌ List indexing not implemented: `df.iloc[[1, 3, 5]]`

---

### Label-Based Indexing (.loc)

#### `DataFrame.loc[key]`

Filter rows by index column values.

**Requires:** Index must be set first

**Supported keys:**
- Single value: `df.loc[12345]`
- Slice: `df.loc[1000:2000]` (inclusive range)
- List: `df.loc[[val1, val2, val3]]`

**Returns:** DataFrame

**Example:**
```python
df = df.set_index('customer_id')

# Single value
customer = df.loc[12345]

# Range
customers = df.loc[10000:20000]

# Multiple values
customers = df.loc[[12345, 67890, 11111]]
```

**SQL Translation:**
```python
df.loc[10000:20000]
# → SELECT * FROM table 
#   WHERE customer_id >= 10000 AND customer_id <= 20000
#   ORDER BY customer_id
```

---

### Convenience Methods

#### `DataFrame.head(n=5)`

Return first n rows based on index ordering.

**Parameters:**
- `n` (int): Number of rows (default: 5)

**Returns:** DataFrame

**Example:**
```python
df.set_index('timestamp').head(10)

# Works without index (arbitrary order)
df.head()
```

---

#### `DataFrame.tail(n=5)`

Return last n rows based on index ordering.

**Requires:** Index must be set

**Parameters:**
- `n` (int): Number of rows (default: 5)

**Returns:** DataFrame

**Example:**
```python
df.set_index('timestamp').tail(10)
```

**Note:** Requires index for deterministic results.

---

### Index Object

#### `DataFrame.index`

Access the index (ordering specification) for the DataFrame.

**Returns:** `Index` object or `None`

**Example:**
```python
df = df.set_index('timestamp', ascending=False)
print(df.index)  # Index('timestamp', descending)
print(df.index.column)  # 'timestamp'
print(df.index.ascending)  # False
```

---

## Working with Nested Data

### Extract then Index

```python
from leanframe.core.frame import DataFrameHandler

# 1. Create handler
handler = DataFrameHandler(nested_df)

# 2. Extract nested fields
flat_df = handler.extract_nested_fields()

# 3. Apply indexing
by_age = flat_df.set_index('person_age', ascending=False)
oldest = by_age.head(10)
```

### Join then Index

```python
from leanframe.core.nested_handler import NestedHandler

# 1. Setup handler
handler = NestedHandler()
handler.add("customers", customers_df)
handler.add("orders", orders_df)

# 2. Join
joined = handler.join(
    tables={"c": "customers", "o": "orders"},
    on=[("c", "customer_id", "o", "customer_id")]
)

# 3. Index and slice
by_date = joined.set_index('order_date', ascending=False)
recent = by_date.iloc[0:100]
```

---

## Common Patterns

### Time-Series: Most Recent Records

```python
df.set_index('timestamp', ascending=False).head(100)
```

### Time-Series: Oldest Records

```python
df.set_index('timestamp', ascending=True).head(100)
```

### Top N by Score

```python
df.set_index('score', ascending=False).iloc[0:10]
```

### Filter by ID Range

```python
df.set_index('customer_id').loc[10000:20000]
```

### Get Specific Records

```python
df.set_index('order_id').loc[[5001, 5002, 5003]]
```

---

## Error Handling

### Using .iloc without index

```python
df.iloc[0]  # ❌ ValueError: Cannot use .iloc without an index
df.set_index('id').iloc[0]  # ✅ OK
```

### Using .loc without index

```python
df.loc[123]  # ❌ ValueError: Cannot use .loc without an index
df.set_index('id').loc[123]  # ✅ OK
```

### Invalid column name

```python
df.set_index('nonexistent')  # ❌ KeyError: Column 'nonexistent' not found
```

---

## Performance Tips

### ✅ DO

- Set index on columns with good selectivity
- Use positive indices: `df.iloc[0:100]`
- Use `.loc` for value filtering: `df.loc[10000:20000]`
- Order by indexed/primary key columns when possible

### ❌ AVOID

- Negative indices: `df.iloc[-1]` (requires full scan)
- Step slicing: `df.iloc[::2]` (not supported)
- Indexing on columns with poor selectivity
- Repeated materialization in loops

---

## Comparison with Pandas

| Feature | Pandas | Leanframe |
|---------|--------|-----------|
| Position access | `df.iloc[0]` | `df.set_index('col').iloc[0]` |
| Label access | `df.loc['label']` | `df.set_index('col').loc[value]` |
| Multi-index | ✅ Supported | ❌ Not supported |
| Implicit order | ✅ Yes | ❌ No (explicit required) |
| Negative indices | ✅ Fast | ⚠️ Slow (full scan) |
| Step slicing | ✅ `df.iloc[::2]` | ❌ Not supported |

---

## See Also

- [Full Indexing Guide](../docs/indexing_guide.md) - Detailed documentation
- [Demo with Nested Data](../demos/demo_indexing_with_nested.py) - Examples
- [Unit Tests](../tests/unit/test_indexing.py) - Test coverage
- [DataFrame Methods Spec](../specs/2025-10-27-dataframe-methods.md)
