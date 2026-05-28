# Indexing Feature - README

## Quick Start

```python
from leanframe.core.frame import DataFrame
import ibis

# Create DataFrame
data = {'id': [1, 2, 3, 4, 5], 'value': [10, 20, 30, 40, 50]}
df = DataFrame(ibis.memtable(data))

# Set index (establishes ordering)
df = df.set_index('id', ascending=True)

# Position-based indexing
first_row = df.iloc[0]           # First row
first_three = df.iloc[0:3]       # First 3 rows
from_third = df.iloc[3:]         # All from 3rd onward

# Label-based indexing
row_with_id_3 = df.loc[3]        # Where id == 3
range_2_to_4 = df.loc[2:4]       # Where id BETWEEN 2 AND 4
specific_ids = df.loc[[1, 3, 5]] # Where id IN (1, 3, 5)

# Convenience methods
top_5 = df.head(5)               # First 5 rows
bottom_5 = df.tail(5)            # Last 5 rows
```

## Key Concepts

### 1. Explicit Ordering Required

Unlike pandas where row order is intrinsic, leanframe requires explicit ORDER BY specification:

```python
# ❌ This fails - no ordering specified
df.iloc[0]  # ValueError: Cannot use .iloc without an index

# ✅ This works - explicit ordering
df.set_index('timestamp', ascending=False).iloc[0]  # Newest record
```

**Why?** SQL databases (like BigQuery) don't have persistent row order. Making it explicit ensures deterministic, reproducible results.

### 2. Index is a Specification, Not Data

The index doesn't store values or create a new column. It's just metadata telling leanframe how to order rows:

```python
df = df.set_index('customer_id')
# No data copied, no new column created
# Just: "when ordering is needed, use customer_id ASC"
```

### 3. Works Seamlessly with Nested Data

Indexing integrates with leanframe's nested column handling:

```python
from leanframe.core.frame import DataFrameHandler

# DataFrame with nested columns
handler = DataFrameHandler(nested_df)

# Extract nested fields
flat_df = handler.extract_nested_fields()

# Index on extracted field
by_age = flat_df.set_index('person_age', ascending=False)
oldest_10 = by_age.head(10)
```

## Documentation

- **[Quick Reference](indexing_quick_ref.md)** - API summary, common patterns
- **[Full Guide](indexing_guide.md)** - Comprehensive documentation
- **[Implementation](indexing_implementation.md)** - Design decisions, architecture

## Examples

See [`demos/demo_indexing_with_nested.py`](../demos/demo_indexing_with_nested.py) for 5 comprehensive examples:

1. Basic indexing (`.iloc`, `.loc`, `.head()`, `.tail()`)
2. Nested data with indexing
3. Joins + indexing (via `NestedHandler`)
4. Chaining operations
5. Error handling

Run it:
```bash
python demos/demo_indexing_with_nested.py
```

## Tests

Run unit tests:
```bash
pytest tests/unit/test_indexing.py -v
```

Coverage includes:
- Index creation and properties
- `.iloc` position-based indexing
- `.loc` label-based indexing
- `.head()` and `.tail()` methods
- Integration with nested data
- Error conditions and edge cases

## API Summary

### Setting Index

```python
df.set_index(column, ascending=True, name=None) → DataFrame
```

### Position-Based (requires index)

```python
df.iloc[0]        # Single row
df.iloc[0:10]     # Slice
df.iloc[10:]      # Open-ended
```

### Label-Based (requires index)

```python
df.loc[value]           # Single value
df.loc[start:end]       # Range (inclusive)
df.loc[[v1, v2, v3]]    # Multiple values
```

### Convenience

```python
df.head(n=5)    # First n rows (with ordering)
df.tail(n=5)    # Last n rows (requires index)
```

## Common Patterns

### Time-Series: Most Recent

```python
df.set_index('timestamp', ascending=False).head(100)
```

### Top N by Score

```python
df.set_index('score', ascending=False).iloc[0:10]
```

### Filter by ID Range

```python
df.set_index('customer_id').loc[10000:20000]
```

### Specific Records

```python
df.set_index('order_id').loc[[5001, 5002, 5003]]
```

## Performance

### Fast ✅
- Setting index (metadata only)
- `.iloc` with positive indices → `LIMIT/OFFSET`
- `.loc` with values → `WHERE` clause
- `.head()` → `LIMIT`

### Slow ⚠️
- `.iloc[-1]` (negative indexing requires full scan)
- Large offsets: `.iloc[1000000:]`

### Not Supported ❌
- Step slicing: `.iloc[::2]`
- List `.iloc`: `.iloc[[1, 3, 5]]` (coming soon)
- Multi-index (intentionally excluded)

## Design Principles

1. **SQL-First** - Every operation maps to SQL (LIMIT, OFFSET, WHERE, ORDER BY)
2. **Explicit** - No hidden assumptions about ordering
3. **Composable** - Works with all leanframe features
4. **Simple** - No multi-index complexity
5. **Familiar** - Pandas-like where possible

## Comparison with Pandas

| Feature | Pandas | Leanframe |
|---------|--------|-----------|
| Position access | `df.iloc[0]` | `df.set_index('col').iloc[0]` |
| Implicit order | ✅ Yes | ❌ No (explicit) |
| Multi-index | ✅ Yes | ❌ No |
| Negative indices | ✅ Fast | ⚠️ Slow |
| Label access | `df.loc[label]` | `df.set_index('col').loc[value]` |

## Files

```
leanframe/
├── core/
│   ├── frame.py              # Updated with indexing support
│   └── indexing.py           # New: Index, ILocIndexer, LocIndexer
├── docs/
│   ├── indexing_guide.md          # Full documentation
│   ├── indexing_quick_ref.md      # API reference
│   ├── indexing_implementation.md # Design details
│   └── indexing_README.md         # This file
├── demos/
│   └── demo_indexing_with_nested.py  # Examples
└── tests/
    └── unit/
        └── test_indexing.py      # Unit tests
```

## Future Enhancements

### Planned
- [ ] List-based `.iloc`: `df.iloc[[1, 3, 5]]`
- [ ] Boolean indexing: `df.loc[df['amount'] > 100]`
- [ ] Reset index: `df.reset_index()`
- [ ] Index persistence across operations

### Possible
- [ ] Composite ordering: `df.set_index(['col1', 'col2'])`
- [ ] Named indices with operations
- [ ] Index statistics and recommendations
- [ ] Smart caching for index metadata

## Getting Help

- **API Questions**: See [Quick Reference](indexing_quick_ref.md)
- **How-To**: See [Full Guide](indexing_guide.md)
- **Design Questions**: See [Implementation Docs](indexing_implementation.md)
- **Examples**: Run `demos/demo_indexing_with_nested.py`
- **Issues**: Check test cases in `tests/unit/test_indexing.py`

## Contributing

When adding indexing features:

1. Keep SQL-first principle - operations should map to SQL
2. Maintain explicit ordering requirement
3. Add tests to `test_indexing.py`
4. Update documentation
5. Add examples to demo file
6. Consider BigQuery performance implications

## License

Copyright 2025 Google LLC, LeanFrame Authors  
Licensed under Apache License 2.0
