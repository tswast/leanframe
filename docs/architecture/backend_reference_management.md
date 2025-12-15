# Backend Reference Management Architecture

## Overview

This document explains how leanframe's `DataFrameHandler` and `NestedHandler` manage backend table references, enabling DataFrames to track their storage location independently.

## Architecture Evolution

### Previous Design (Centralized)
- **Problem**: NestedHandler managed table qualifiers separately in `_table_qualifiers` dict
- **Issue**: Tight coupling between orchestrator and backend state
- **Limitation**: DataFrame couldn't update its own backend reference

### Current Design (Property-Based)
- **Solution**: DataFrameHandler owns its backend reference as a property
- **Benefit**: Clean separation of concerns
- **Feature**: Independent backend status updates

## Core Components

### DataFrameHandler

Each `DataFrameHandler` instance manages:
- Its DataFrame and nested structure analysis
- Its backend table reference (optional `table_qualifier`)
- Methods to query and update backend status

```python
handler = DataFrameHandler(df, table_qualifier="mydb.sales.customers")

# Query backend status
handler.table_qualifier           # "mydb.sales.customers"
handler.has_backend_table()       # True
handler.get_backend_info()        # Parsed qualifier details

# Update backend reference
handler.set_table_qualifier("new.location.customers")  # Update
handler.set_table_qualifier(None)                      # Clear (in-memory)
```

### NestedHandler

The orchestrator manages multiple DataFrameHandlers:
- Stores handlers by user-friendly names
- Coordinates operations (joins, etc.)
- Accesses backend references via handler properties

```python
nested = NestedHandler()

# Add with backend reference
nested.add("customers", df, table_qualifier="mydb.sales.customers")

# Access handler and its backend info
handler = nested.get("customers")
print(handler.table_qualifier)    # "mydb.sales.customers"

# Show all backend statuses
nested.show_backend_status()
```

## Key Properties & Methods

### DataFrameHandler

#### `table_qualifier` Property
```python
@property
def table_qualifier(self) -> str | None:
    """Get backend table qualifier or None for in-memory DataFrame."""
```

- **Returns**: Backend identifier (e.g., `"project.dataset.table"`) or `None`
- **Example**: `"local_duckdb.main.customers"`

#### `set_table_qualifier()` Method
```python
def set_table_qualifier(self, qualifier: str | None):
    """Update backend reference. Call when DataFrame saved/dropped."""
```

- **Usage**: Update when backend state changes
- **Events**: Save to backend, load from backend, drop table
- **Prints**: Status messages showing reference changes

#### `has_backend_table()` Method
```python
def has_backend_table(self) -> bool:
    """Check if DataFrame has backend table reference."""
```

- **Returns**: `True` if backend table exists, `False` for in-memory
- **Use Case**: Conditional logic based on backend status

#### `get_backend_info()` Method
```python
def get_backend_info(self) -> dict[str, str | None]:
    """Get parsed backend information."""
```

- **Returns**: Dict with `qualifier`, `project`, `dataset`, `table`, `type`
- **Parsing**: Attempts to parse standard `project.dataset.table` format
- **Example**:
  ```python
  {
      'qualifier': 'myproject.sales.customers',
      'project': 'myproject',
      'dataset': 'sales',
      'table': 'customers',
      'type': 'backend'
  }
  ```

### NestedHandler

#### `show_backend_status()` Method
```python
def show_backend_status(self):
    """Display backend status for all DataFrames."""
```

- **Output**: Pretty-printed list showing each DataFrame's backend status
- **Symbols**: ✅ for backend tables, ⚪ for in-memory DataFrames

## Lifecycle Examples

### 1. In-Memory → Backend → In-Memory

```python
# Start with in-memory DataFrame
nested = NestedHandler()
nested.add("temp", df)  # No table_qualifier

handler = nested.get("temp")
print(handler.has_backend_table())  # False

# Save to backend
backend.create_table("temp", df.to_pandas())
handler.set_table_qualifier("mydb.main.temp")
print(handler.has_backend_table())  # True

# Drop from backend (DataFrame still in memory)
backend.drop_table("temp")
handler.set_table_qualifier(None)
print(handler.has_backend_table())  # False
```

### 2. Load from Backend

```python
# Load from database with qualifier
df = session.read_sql_table("customers")
nested.add("customers", df, table_qualifier="mydb.sales.customers")

handler = nested.get("customers")
print(handler.table_qualifier)  # "mydb.sales.customers"
```

### 3. Join Result Lineage

```python
# Join tracks source tables in qualifier
nested.add("customers", customers_df, table_qualifier="db.sales.customers")
nested.add("orders", orders_df, table_qualifier="db.sales.orders")

result, name = nested.join_on_nested(
    left="customers",
    right="orders",
    left_path="email",
    right_column="customer_email"
)

handler = nested.get("customers_orders_joined")
print(handler.table_qualifier)
# Output: "joined(db.sales.customers⋈db.sales.orders)"
```

## Design Benefits

### ✅ Clean Separation of Concerns
- **DataFrameHandler**: Manages individual DataFrame state (including backend reference)
- **NestedHandler**: Orchestrates operations across DataFrames
- **No Duplication**: Single source of truth for backend reference

### ✅ Independent Updates
- DataFrame can update its own backend status
- No need to notify or update orchestrator
- Natural flow: save → update reference, drop → clear reference

### ✅ Flexible Storage
- `None` for in-memory DataFrames
- Qualified names for backend tables: `"project.dataset.table"`
- Custom formats for lineage: `"joined(table1⋈table2)"`

### ✅ Lineage Tracking
- Join results track source tables
- Easy to see data provenance
- Supports complex workflows with multiple joins

### ✅ Backend Agnostic
- Works with any backend (DuckDB, BigQuery, etc.)
- Qualifier format adapts to backend conventions
- Parser supports multiple formats (3-part, 2-part, 1-part)

## Best Practices

### 1. Set Qualifier When Loading from Backend
```python
df = load_from_database("customers")
nested.add("customers", df, table_qualifier="mydb.sales.customers")
```

### 2. Update After Saving to Backend
```python
# Create in-memory
nested.add("result", result_df)  # No qualifier

# Save and update
backend.create_table("result", result_df.to_pandas())
nested.get("result").set_table_qualifier("mydb.temp.result")
```

### 3. Clear After Dropping from Backend
```python
backend.drop_table("temp")
nested.get("temp").set_table_qualifier(None)
# DataFrame still available in memory!
```

### 4. Check Before Backend Operations
```python
handler = nested.get("data")
if handler.has_backend_table():
    # Can reload from backend
    backend.table(handler.table_qualifier)
else:
    # In-memory only
    print("No backend table available")
```

## Future Enhancements

### Automatic Synchronization
- Notification system for backend events (save/drop)
- Auto-update references when backend changes
- Callback registration for lifecycle events

### Smart Reload
- Reload DataFrame from backend using qualifier
- Refresh stale data
- Validate backend table still exists

### Advanced Lineage
- Track full operation history
- Serialize/deserialize lineage graphs
- Visualize data provenance

### Backend Validation
- Verify qualifier matches backend schema
- Auto-detect qualifier format
- Validate table exists before setting reference

## Summary

The property-based backend reference management architecture provides:

1. **Ownership**: DataFrameHandler owns its backend reference
2. **Independence**: Can update without coordinating with NestedHandler
3. **Flexibility**: Supports in-memory, backend, and hybrid workflows
4. **Lineage**: Tracks data provenance through qualifiers
5. **Simplicity**: Clean API with intuitive methods

This design enables complex workflows while maintaining clean separation of concerns and allowing DataFrames to independently manage their backend state.
