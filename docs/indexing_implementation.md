# Indexing Implementation Summary

## What We Built

A pandas-like indexing system for leanframe that's SQL-friendly and works seamlessly with BigQuery and nested data handling.

## Files Created

### 1. Core Implementation
- **`leanframe/core/indexing.py`** - Main indexing module
  - `Index` class - Ordering specification (not data storage)
  - `ILocIndexer` - Position-based indexing (`.iloc`)
  - `LocIndexer` - Label-based indexing (`.loc`)
  - `HeadTailMixin` - Convenience methods (`.head()`, `.tail()`)

### 2. Documentation
- **`docs/indexing_guide.md`** - Comprehensive guide
  - Core concepts and philosophy
  - Detailed API documentation
  - Working with nested data
  - Best practices and patterns
  - Migration guide from pandas

- **`docs/indexing_quick_ref.md`** - Quick reference
  - API summary
  - Common patterns
  - Error handling
  - Performance tips

### 3. Examples & Tests
- **`demos/demo_indexing_with_nested.py`** - Interactive demos
  - 5 comprehensive examples
  - Shows integration with nested data handling
  - Error handling demonstrations

- **`tests/unit/test_indexing.py`** - Unit tests
  - 30+ test cases
  - Coverage for all indexing features
  - Edge cases and error conditions
  - Integration with nested data

### 4. Integration
- **Updated `leanframe/core/frame.py`**
  - Added `Index`, `ILocIndexer`, `LocIndexer` imports
  - Inherited `HeadTailMixin` in `DataFrame` class
  - Added `.index`, `.iloc`, `.loc` properties
  - Added `.set_index()` method
  - Initialized indexing state in `__init__`

## Key Design Decisions

### 1. Explicit Ordering (Not Implicit)

**Problem:** SQL databases don't have intrinsic row order like pandas.

**Solution:** Require explicit ORDER BY specification via `.set_index()`.

```python
# ❌ Pandas: assumes intrinsic order
df.iloc[0:10]

# ✅ Leanframe: explicit order
df.set_index('timestamp', ascending=False).iloc[0:10]
```

**Why:** This forces users to think about ordering, preventing non-deterministic results.

### 2. Index as Specification (Not Data)

**Problem:** Pandas Index stores actual values; impractical for large SQL tables.

**Solution:** Index is a lightweight specification for ORDER BY clauses.

```python
class Index:
    def __init__(self, column: str, ascending: bool = True, name: str | None = None):
        self.column = column      # Which column to order by
        self.ascending = ascending  # ASC or DESC
        self.name = name           # Optional name
```

**Why:** No data materialization; just metadata for SQL generation.

### 3. Single Index Only (No Multi-Index)

**Problem:** Multi-index adds complexity and doesn't map cleanly to SQL.

**Solution:** Support only single-column ordering.

**Future:** Could extend to composite ordering via:
```python
df.set_index(['priority', 'timestamp'], ascending=[False, True])
```

### 4. SQL-First Operation Mapping

Every indexing operation maps directly to SQL:

| Operation | SQL Translation |
|-----------|-----------------|
| `df.iloc[0:10]` | `LIMIT 10 OFFSET 0` |
| `df.iloc[10:20]` | `LIMIT 10 OFFSET 10` |
| `df.loc[123]` | `WHERE col = 123` |
| `df.loc[100:200]` | `WHERE col BETWEEN 100 AND 200` |
| `df.head(10)` | `LIMIT 10` |
| `df.tail(10)` | `LIMIT 10 (reversed order)` |

**Why:** Predictable performance; users understand what's happening.

### 5. Integration with Nested Data

Indexing works seamlessly with existing nested data handling:

```python
# Extract nested fields
handler = DataFrameHandler(nested_df)
flat_df = handler.extract_nested_fields()

# Apply indexing
by_age = flat_df.set_index('person_age', ascending=False)
oldest = by_age.head(10)
```

**Why:** Composability - each feature works independently and together.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      DataFrame                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Properties:                                            │ │
│  │  - .index → Index (ordering spec)                     │ │
│  │  - .iloc → ILocIndexer (position-based)               │ │
│  │  - .loc → LocIndexer (label-based)                    │ │
│  │                                                        │ │
│  │ Methods:                                               │ │
│  │  - .set_index(col, asc) → DataFrame                   │ │
│  │  - .head(n) → DataFrame                               │ │
│  │  - .tail(n) → DataFrame                               │ │
│  └────────────────────────────────────────────────────────┘ │
│                           │                                  │
│                           │ uses                             │
│                           ▼                                  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │                    Index                               │ │
│  │  - column: str                                         │ │
│  │  - ascending: bool                                     │ │
│  │  - name: str                                           │ │
│  └────────────────────────────────────────────────────────┘ │
│                           │                                  │
│                           │ used by                          │
│                  ┌────────┴────────┐                        │
│                  ▼                 ▼                         │
│  ┌──────────────────┐  ┌──────────────────┐               │
│  │  ILocIndexer     │  │  LocIndexer      │               │
│  │  - [int]         │  │  - [value]       │               │
│  │  - [slice]       │  │  - [slice]       │               │
│  │                  │  │  - [list]        │               │
│  └──────────────────┘  └──────────────────┘               │
└─────────────────────────────────────────────────────────────┘
                           │
                           │ generates
                           ▼
                ┌─────────────────────┐
                │   Ibis SQL          │
                │  - ORDER BY         │
                │  - LIMIT/OFFSET     │
                │  - WHERE            │
                └─────────────────────┘
```

## Usage Examples

### Basic Indexing

```python
# Create DataFrame
df = DataFrame(ibis.memtable({'id': [1, 2, 3], 'value': [10, 20, 30]}))

# Set index
df = df.set_index('id')

# Position-based
first = df.iloc[0]      # First row
top_2 = df.iloc[0:2]    # First 2 rows

# Label-based
row = df.loc[2]         # Where id=2
range_rows = df.loc[1:3]  # Where id BETWEEN 1 AND 3

# Convenience
first_5 = df.head(5)
last_5 = df.tail(5)
```

### With Nested Data

```python
from leanframe.core.frame import DataFrameHandler

# Nested DataFrame
nested_df = DataFrame(ibis.memtable({
    'id': [1, 2, 3],
    'person': [
        {'name': 'Alice', 'age': 30},
        {'name': 'Bob', 'age': 25},
        {'name': 'Carol', 'age': 35}
    ]
}))

# Extract nested fields
handler = DataFrameHandler(nested_df)
flat_df = handler.extract_nested_fields()

# Index on extracted field
by_age = flat_df.set_index('person_age', ascending=False)
oldest = by_age.iloc[0]  # Carol
```

### With Joins

```python
from leanframe.core.nested_handler import NestedHandler

# Setup
handler = NestedHandler()
handler.add("customers", customers_df)
handler.add("orders", orders_df)

# Join
joined = handler.join(
    tables={"c": "customers", "o": "orders"},
    on=[("c", "customer_id", "o", "customer_id")]
)

# Index and slice
by_date = joined.set_index('order_date', ascending=False)
recent_100 = by_date.iloc[0:100]
```

## Performance Characteristics

### Fast Operations ✅
- Setting index (metadata only)
- `.iloc` with positive indices
- `.loc` with single value or range
- `.head()` with explicit index

### Slow Operations ⚠️
- `.iloc` with negative indices (requires full scan)
- `.tail()` without index (error)
- Large offset in `.iloc[1000000:]`

### Not Supported ❌
- Step slicing: `df.iloc[::2]`
- List-based `.iloc`: `df.iloc[[1, 3, 5]]`
- Multi-index

## Testing

Run the tests:

```bash
# All indexing tests
pytest tests/unit/test_indexing.py -v

# Specific test class
pytest tests/unit/test_indexing.py::TestILocIndexing -v

# Run demos
python demos/demo_indexing_with_nested.py
```

## Next Steps

### Immediate Enhancements
1. **List-based .iloc**: `df.iloc[[1, 3, 5]]` using row_number() window function
2. **Boolean indexing**: `df.loc[df['amount'] > 100]`
3. **Index persistence**: Keep index across operations like `.assign()`
4. **Reset index**: `df.reset_index()` method

### Future Features
1. **Composite ordering**: Multi-column index via tuple
2. **Named indices**: Better support for index names
3. **Index operations**: `.reindex()`, `.sort_index()`
4. **Integration with groupby**: Use index in aggregations

### Advanced Features
1. **Optimized negative indexing**: Use window functions instead of reversing
2. **Index caching**: Cache index metadata across sessions
3. **Smart index selection**: Auto-suggest best index column
4. **Index statistics**: Show coverage, cardinality, etc.

## Design Philosophy

Leanframe's indexing embodies **SQL-first thinking**:

1. **Explicit over Implicit** - No hidden assumptions
2. **Predictable Performance** - Operations map to known SQL patterns
3. **Composability** - Works with all existing features
4. **Simplicity** - No multi-index complexity
5. **Familiarity** - Pandas-like API where possible

This makes code:
- **Portable** - Easy to translate to pure SQL
- **Debuggable** - Clear what's happening under the hood
- **Scalable** - No performance surprises
- **Maintainable** - Simple mental model

## Conclusion

We've built a complete indexing system that:
- ✅ Provides pandas-like API (`.iloc`, `.loc`, `.head()`, `.tail()`)
- ✅ Works with SQL databases (BigQuery, etc.)
- ✅ Integrates with nested data handling
- ✅ Has clear performance characteristics
- ✅ Is well-documented and tested
- ✅ Follows SQL-first principles

The system is **production-ready** for basic use cases and has a clear path for future enhancements.
