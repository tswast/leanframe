## Joining on Nested Columns: Strategy & Implementation Guide

### Problem: How to join DataFrames on nested columns at arbitrary depth?

You have nested structures in BigQuery/Ibis and want to join on deeply nested fields like `customer.profile.contact.email` with regular columns like `order.customer_email`.

---

## 🏗️ NEW ARCHITECTURE: NestedHandler + DataFrameHandler

We've refactored to a clean two-class architecture following Single Responsibility Principle:

### DataFrameHandler (Individual Wrapper)
- Wraps **ONE** leanframe DataFrame
- Introspects and caches schema metadata
- Provides extraction methods for nested fields
- Functional operations (no data caching)

### NestedHandler (Orchestrator)
- Manages **MULTIPLE** DataFrameHandler instances
- Coordinates cross-DataFrame operations (joins, etc.)
- Fluent API: `add()` → `join_on_nested()`
- Clean separation of concerns

### Quick Example:
```python
from leanframe.core.nested_handler import NestedHandler

# Create orchestrator
handler = NestedHandler()

# Add DataFrames (creates DataFrameHandler automatically)
handler.add("customers", customers_df)
handler.add("orders", orders_df)

# Perform join using names
joined_df = handler.join_on_nested(
    left="customers",
    right="orders",
    left_path="profile.contact.email",
    right_column="customer_email",
    how="inner"
)

# Result can be added back for chaining
handler.add("customer_orders", joined_df)
```

---

## ✅ RECOMMENDED APPROACH: Selective Flattening

### Core Strategy:
1. **Keep data nested by default** (efficient storage/transfer)
2. **Extract only join columns** (minimal flattening)  
3. **Use NestedHandler for orchestration** (manages multiple DataFrames)
4. **DataFrameHandler for auto-discovery** (handles arbitrary depth)
5. **Join on flattened keys** (BigQuery-compatible)
6. **Preserve nested structure in results** (best of both worlds)

### Why This Works Best:
- ✅ **BigQuery Native**: Works with BigQuery's SQL engine
- ✅ **Efficient**: Only extracts what's needed for joins
- ✅ **Flexible**: Handles any nesting depth dynamically
- ✅ **Scalable**: Avoids massive column explosion
- ✅ **Ibis Compatible**: Uses standard Ibis join operations

---

## 🔧 Implementation Pattern

### 1. Dynamic Structure Discovery
```python
from leanframe.core.frame import DataFrameHandler

# Create handler once - introspects schema, caches metadata
handler = DataFrameHandler(nested_df)

# Fast metadata access (cached, no computation)
print(handler.extracted_fields)
# {'profile.contact.email': 'profile_contact_email', 
#  'profile.contact.region': 'profile_contact_region', ...}

handler.show_structure()  # Fast - displays cached metadata

# Key Design Principle:
# - Handler caches IMMUTABLE schema metadata (thread-safe, efficient)
# - Handler does NOT cache data (functional operations, no stale state)
# - Create once per DataFrame, reuse for multiple operations
```

### 2. Selective Extraction for Joins
```python
class NestedJoinHelper:
    @staticmethod
    def prepare_for_join(handler: DataFrameHandler, join_path: str, prefix: str = ""):
        """Extract only the nested field needed for joining.
        
        Args:
            handler: DataFrameHandler instance (reuse to avoid re-introspection!)
            join_path: Nested path like "profile.contact.email"
            prefix: Optional prefix for extracted column name
            
        Design Note:
            - Handler caches schema metadata (fast lookup in extracted_fields)
            - extract_nested_fields() is FUNCTIONAL (no data caching)
            - Reuse handler across multiple operations for efficiency
        """
        if join_path in handler.extracted_fields:
            extracted_name = handler.extracted_fields[join_path]  # Fast metadata lookup
            final_name = f"{prefix}_{extracted_name}" if prefix else extracted_name
            
            # Functional operation - computes fresh result each time
            extracted_df = handler.extract_nested_fields()
            return extracted_df, final_name
        else:
            raise ValueError(f"Join path '{join_path}' not found")
```

### 3. Ibis Join Operation
```python
def join_on_nested(left_handler, right_df, left_nested_path, right_column, how="inner"):
    """Join on nested column by extracting it first.
    
    Args:
        left_handler: DataFrameHandler instance (pass existing, don't create new!)
        right_df: DataFrame to join with
        left_nested_path: Nested path to extract and join on
        right_column: Flat column name in right_df
        how: Join type ("inner", "left", "outer", etc.)
    
    Design Pattern:
        - Pass handler as parameter (reuse existing instance)
        - Extract fields functionally (no cached state)
        - Efficient: Schema already analyzed in handler
    """
    # Prepare left side - uses cached metadata, computes fresh data
    left_prepared, left_join_col = NestedJoinHelper.prepare_for_join(
        left_handler, left_nested_path, "left"
    )
    
    # Join using Ibis
    joined_table = left_prepared._data.join(
        right_df._data,
        predicates=[(left_join_col, right_column)],
        how=how
    )
    
    return DataFrame(joined_table)
```

### 4. Complete Example
```python
# Create handler once - caches schema metadata
customer_handler = DataFrameHandler(customers_df)
customer_handler.show_structure()  # Fast - uses cached metadata

# Join using handler - functional data extraction
joined_df = NestedJoinHelper.join_on_nested(
    left_handler=customer_handler,    # Pass handler (not DataFrame!)
    right_df=orders_df,               # Has customer_email  
    left_nested_path="profile.contact.email",  # Uses cached metadata lookup
    right_join_column="customer_email",
    how="inner"
)

# Reuse same handler for multiple operations - no re-introspection needed!
# Result preserves nested structures + adds flat join columns
```

---

## 📊 Strategy Comparison

| Approach | Pros | Cons | BigQuery Fit |
|----------|------|------|--------------|
| **🚀 Selective Flattening** | ✅ Efficient<br>✅ Flexible<br>✅ Preserves structure | ⚠️ Requires extraction logic | ⭐⭐⭐⭐⭐ |
| **🐌 Full Flattening** | ✅ Simple joins | ❌ Column explosion<br>❌ Hard to re-nest | ⭐⭐⭐ |
| **🔧 Native Struct Joins** | ✅ No flattening | ❌ Limited BigQuery support<br>❌ Complex syntax | ⭐⭐ |

---

## 🎯 Key Benefits of Our Approach

### For Your Use Case:
1. **DataFrameHandler discovers structure automatically** - works with any nesting
2. **Selective extraction** - only flatten join keys, keep rest nested  
3. **BigQuery native** - uses standard SQL joins under the hood
4. **Preserves performance** - minimal data movement and processing
5. **Handles arbitrary depth** - works with `level1.level2.level3.field` joins

### Real-World Example:
```python
# Customer data: profile.contact.email (nested 2 levels deep)
# Order data: customer_email (flat)
# Result: Efficient join preserving customer's nested profile structure
```

---

## 📝 Implementation Notes

### When to Use This Pattern:
- ✅ Joining on nested fields of any depth
- ✅ BigQuery/Ibis backend  
- ✅ Want to preserve nested structures
- ✅ Need flexible, dynamic solutions

### Alternative Simple Cases:
- **Single-level nesting**: Can use direct Ibis field access: `table["struct_col"]["field"]`
- **Known structure**: Can pre-extract specific fields manually
- **Full flattening OK**: Use `handler.extract_nested_fields()` for all operations

### Performance Considerations:
- **Schema introspection**: One-time cost when creating handler (cached as metadata)
- **Data extraction**: Functional operation - computes fresh each time (no stale state)
- **Handler reuse**: Avoid re-introspection by passing handlers as parameters
- **Storage efficiency**: Nested data stays compact
- **Query efficiency**: BigQuery optimizes struct field access
- **Network efficiency**: Less data transfer vs full flattening
- **Thread safety**: Metadata caching is safe; no data caching = no race conditions

---

## 🔍 Demo Results

The demo shows successful:
- ✅ **Auto-discovery** of nested structures (`profile.contact.email`, `order_details.shipping.method`)
- ✅ **Dynamic extraction** of join keys without manual field specification  
- ✅ **Join execution** using standard Ibis operations
- ✅ **Structure preservation** of non-join nested columns

**Recommendation**: Use this selective flattening pattern with DataFrameHandler for robust, efficient nested joins in BigQuery environments.