# Fixing DuckDB "Table Does Not Exist" Error in Nested Joins

## Problem

When using `DataFrameHandler` with Ibis/DuckDB backend for joins, you encountered:
```
Catalog Error: Table with name lf_litdmfcxzm does not exist!
```

## Root Causes

### 1. Multiple Handler Creation (Primary Issue)
**Don't create new `DataFrameHandler` instances unnecessarily!** Each handler creation:
- Triggers expensive introspection
- Creates new temporary table references
- Can orphan table references across operations

### 2. Multiple Sessions (Secondary Issue)
**Each new DuckDB session creates its own isolated catalog**. When you:

1. Create DataFrame A in session 1 → generates temp table `lf_abc123`
2. Create DataFrame B in session 2 → generates temp table `lf_def456`  
3. Join A and B → creates reference to both tables
4. Create new `DataFrameHandler` on joined result → **new session 3**
5. Try to access data → ❌ **tables from sessions 1 & 2 don't exist in session 3!**

The problem occurs because:
- `pyarrow_to_leanframe()` created a **new** DuckDB connection each time
- Each DataFrame had references to tables in **different** DuckDB instances
- Joined results referenced tables that were orphaned across sessions

## Solutions

### Solution 1: Reuse Handlers (MOST IMPORTANT!)

**Create handler ONCE, reuse everywhere:**

```python
# ❌ BAD: Creating new handlers repeatedly
def join_dataframes(df1, df2, join_path):
    handler1 = DataFrameHandler(df1)  # Creates temp tables
    # ... extract fields ...
    handler2 = DataFrameHandler(df1)  # ❌ WASTEFUL! Re-creates everything
    # ... more operations ...
    handler3 = DataFrameHandler(joined)  # ❌ Can fail with table refs

# ✅ GOOD: Create once, reuse
customer_handler = DataFrameHandler(customers_df)  # Create ONCE (schema introspection)
customer_handler.show_structure()  # Inspect cached metadata (fast!)
print(handler.extracted_fields)  # Access cached schema info

extracted_df = customer_handler.extract_nested_fields()  # Compute when needed (functional!)
join_on_nested(customer_handler, orders_df, ...)  # Pass handler (metadata), not data
```

**Key principles**: 
- Handler caches **IMMUTABLE schema metadata** (fast, thread-safe)
- Handler does **NOT cache data** (functional operations, no stale state)
- Create handler once per DataFrame, reuse for multiple operations

### Solution 2: Shared Session Pattern

**Use a single shared session for all operations in your workflow:**

```python
import ibis
import leanframe

# Global shared session
_shared_session = None

def get_shared_session():
    """Get or create a shared leanframe session for all operations."""
    global _shared_session
    if _shared_session is None:
        backend = ibis.duckdb.connect()
        _shared_session = leanframe.Session(backend=backend)
    return _shared_session

# Usage
session = get_shared_session()
df1 = session.DataFrame(pandas_df1)  # Uses same backend
df2 = session.DataFrame(pandas_df2)  # Uses same backend
joined = join_operation(df1, df2)     # Tables exist in same catalog!
```

## Key Benefits

✅ **Single catalog**: All temporary tables exist in one DuckDB instance  
✅ **Consistent references**: Joins maintain valid table references  
✅ **No orphaned tables**: All DataFrames share the same backend  
✅ **Proper lifecycle**: Tables persist for the session duration

## Alternative: Materialize Before Creating New Handlers

If you can't use a shared session, materialize the data:

```python
# Instead of creating new handler on joined result:
joined_handler = DataFrameHandler(joined_df)  # ❌ May fail

# Materialize first:
joined_pandas = joined_df.to_pandas()  # ✅ Materializes data
# Then work with pandas data directly
```

## Implementation in demo_nested_joins.py

**Before (Broken):**
```python
# ❌ Created new handlers repeatedly
def prepare_for_join(df, join_path):
    handler = DataFrameHandler(df)  # New handler every call!
    # ...

def join_operation():
    joined_df = join_dataframes(...)
    handler = DataFrameHandler(joined_df)  # Another new handler!
```

**After (Fixed):**
```python
# ✅ Create handlers once, reuse them
customer_handler = DataFrameHandler(customers_df)  # Once!
customer_handler.show_structure()

# Pass handler to functions, don't recreate
joined_df = NestedJoinHelper.join_on_nested(
    left_handler=customer_handler,  # Reuse existing handler!
    right_df=orders_df,
    left_join_path="profile.contact.email",
    ...
)

# ✅ For sessions: Use shared session
session = get_shared_session()  # Reuse same session
customers_df = session.DataFrame(customer_pandas)
orders_df = session.DataFrame(order_pandas)  # Same catalog
```

## When This Matters

This issue affects:
- ✅ **Joins across DataFrames** from different creation points
- ✅ **Creating handlers on joined results** 
- ✅ **Any multi-step DataFrame operations** in DuckDB
- ❌ Not an issue with BigQuery backend (server-side tables)

## Best Practices

### Universal (DuckDB & BigQuery):
1. **✅ Create handlers ONCE per DataFrame** - Don't recreate unnecessarily
2. **✅ Pass handlers as parameters** - Reuse existing instances in functions
3. **✅ Avoid creating handlers on intermediate results** - Use the original handler's extracted_df instead

### DuckDB Specific:
4. **✅ Use shared session** - One session for all related DataFrames
5. **✅ Materialize if needed** - Use `.to_pandas()` before creating new handlers on complex results

### BigQuery Production:
- Handler reuse still matters for **performance** (avoid re-introspection)
- Session isolation less critical (server-side tables)
- But shared session is still **good practice** for consistency

## Performance Impact

**Creating unnecessary handlers costs:**
- 🐌 Re-introspection of schema (can be slow on large nested structures)
- 🐌 DuckDB temp table management overhead
- 💾 Memory for duplicate schema metadata

**Handler reuse gives:**
- 🚀 Instant access to cached schema metadata
- 🚀 No redundant schema analysis
- 🚀 Clean table reference management
- ✨ Better code clarity

**Note on data operations:**
- `extract_nested_fields()` is a **functional operation** - always computes fresh results
- No data caching = no stale state, safe for parallel operations
- Only schema metadata is cached (immutable, thread-safe)