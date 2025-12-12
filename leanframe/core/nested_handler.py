# Copyright 2025 Google LLC, LeanFrame Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
NestedHandler - Orchestrator for multi-DataFrame operations with nested columns

This module provides the NestedHandler class which manages multiple DataFrames
and coordinates operations between them, particularly joins on nested columns.
"""

from __future__ import annotations

from leanframe.core.frame import DataFrame, DataFrameHandler


class NestedHandler:
    """
    Orchestrator for managing multiple DataFrames with nested columns.
    
    This class:
    - Manages multiple DataFrameHandler instances (one per DataFrame)
    - Provides operations across DataFrames (joins, etc.)
    - Maintains relationships between DataFrames
    - Tracks DataFrames by name for easy reference
    
    Design Philosophy:
    - Each DataFrame gets its own DataFrameHandler (single responsibility)
    - NestedHandler coordinates operations between handlers
    - Handlers are created lazily when DataFrames are added
    - Results can be added back to the context for chaining operations
    
    Usage Pattern:
        # Create orchestrator
        handler = NestedHandler()
        
        # Add DataFrames (creates DataFrameHandler internally)
        handler.add("customers", customers_df)
        handler.add("orders", orders_df)
        
        # Perform operations using named references
        joined_df = handler.join_on_nested(
            left="customers",
            right="orders",
            left_path="profile.contact.email",
            right_column="customer_email",
            how="inner"
        )
        
        # Add result back for further operations
        handler.add("customer_orders", joined_df)
    
    Example Workflow:
        handler = NestedHandler()
        handler.add("customers", customers_df)
        handler.add("orders", orders_df)
        handler.add("regions", regions_df)
        
        # Chain operations
        handler.join_on_nested("customers", "regions", ...)
        handler.join_on_nested("customer_regions", "orders", ...)
    """
    
    def __init__(self):
        """Initialize empty NestedHandler."""
        # Maps name -> DataFrameHandler
        self._handlers: dict[str, DataFrameHandler] = {}
        
        # Optional: Track join relationships for lineage/debugging
        self._relationships: list[dict] = []
    
    def add(
        self, 
        name: str, 
        df: DataFrame, 
        max_depth: int = 10,
        table_qualifier: str | None = None
    ) -> DataFrameHandler:
        """
        Add a leanframe DataFrame to the handler context.
        
        This creates a DataFrameHandler for the DataFrame and stores it
        under the given name for later reference in operations.
        
        Args:
            name: Unique identifier for this DataFrame
            df: leanframe DataFrame to add
            max_depth: Maximum nesting depth to analyze (passed to DataFrameHandler)
            table_qualifier: Optional backend table identifier (e.g., "project.dataset.table")
                           The handler will track this as its backend reference.
            
        Returns:
            The created DataFrameHandler (for direct access if needed)
            
        Raises:
            ValueError: If name already exists
            
        Example:
            handler = NestedHandler()
            
            # Without table qualifier (in-memory DataFrame)
            customer_handler = handler.add("customers", customers_df)
            print(customer_handler.table_qualifier)  # None
            
            # With table qualifier for backend reference
            handler.add("orders", orders_df, table_qualifier="mydb.sales.orders")
            
            # Can access handler directly or via name
            print(customer_handler.nested_fields)
            print(handler.get("customers").nested_fields)  # Same thing
            
            # Check backend status
            orders_handler = handler.get("orders")
            print(orders_handler.table_qualifier)  # "mydb.sales.orders"
            print(orders_handler.has_backend_table())  # True
        """
        if name in self._handlers:
            raise ValueError(
                f"DataFrame '{name}' already exists. "
                f"Use remove('{name}') first or choose a different name."
            )
        
        print(f"\n📦 Adding DataFrame '{name}' to NestedHandler...")
        if table_qualifier:
            print(f"   Backend table: {table_qualifier}")
        
        # Create handler with table qualifier - handler owns this reference now
        df_handler = DataFrameHandler(df, max_depth=max_depth, table_qualifier=table_qualifier)
        self._handlers[name] = df_handler
        
        print(f"✅ Added '{name}' with {len(df_handler.original_columns)} columns "
              f"({len(df_handler.nested_fields)} nested fields discovered)")
        
        return df_handler
    
    def get(self, name: str) -> DataFrameHandler:
        """
        Get a DataFrameHandler by name.
        
        Args:
            name: Name of the DataFrame
            
        Returns:
            DataFrameHandler for the named DataFrame
            
        Raises:
            KeyError: If name not found
        """
        if name not in self._handlers:
            available = list(self._handlers.keys())
            raise KeyError(
                f"DataFrame '{name}' not found. "
                f"Available: {available}"
            )
        return self._handlers[name]
    
    def remove(self, name: str):
        """
        Remove a DataFrame from the handler.
        
        Args:
            name: Name of the DataFrame to remove
            
        Raises:
            KeyError: If name not found
        """
        if name not in self._handlers:
            raise KeyError(f"DataFrame '{name}' not found")
        del self._handlers[name]
        print(f"🗑️  Removed DataFrame '{name}' from NestedHandler")
    
    def list_dataframes(self) -> list[str]:
        """
        List all DataFrame names in the handler.
        
        Returns:
            List of DataFrame names
        """
        return list(self._handlers.keys())
    
    def show_backend_status(self):
        """
        Show backend table status for all DataFrames.
        
        Displays which DataFrames have backend tables and which are in-memory only.
        
        Example output:
            📊 Backend Status for 4 DataFrames:
            ✅ customers → myproject.sales.customers
            ✅ orders → myproject.sales.orders
            ⚪ processed → in-memory (no backend table)
            ✅ joined_result → joined(customers⋈orders)
        """
        print(f"\n📊 Backend Status for {len(self._handlers)} DataFrames:")
        for name, handler in self._handlers.items():
            if handler.has_backend_table():
                print(f"✅ {name} → {handler.table_qualifier}")
            else:
                print(f"⚪ {name} → in-memory (no backend table)")
    
    def show_structure(self, name: str | None = None):
        """
        Show structure of one or all DataFrames.
        
        Args:
            name: Specific DataFrame name, or None to show all
        """
        if name is not None:
            print(f"\n📊 Structure of '{name}':")
            self.get(name).show_structure()
        else:
            print(f"\n📊 NestedHandler contains {len(self._handlers)} DataFrames:")
            for df_name in self._handlers.keys():
                print(f"\n--- {df_name} ---")
                self._handlers[df_name].show_structure()
    
    # Data preparation - extract nested fields for operations
    
    def prepare(
        self,
        name: str,
        fields: list[str] | None = None,
        verbose: bool = False
    ) -> DataFrame:
        """
        Prepare a DataFrame by extracting specified nested fields.
        
        This is a preprocessing step before operations like joins. The handler
        analyzes nested structure and extracts fields, returning a flattened
        DataFrame ready for SQL-like operations.
        
        Args:
            name: Name of the DataFrame to prepare
            fields: List of nested paths to extract (e.g., ['profile.email', 'address.city'])
                   If None, extracts all discovered nested fields
            verbose: Whether to print extraction details
            
        Returns:
            DataFrame with nested fields extracted as flat columns
            
        Example:
            # Prepare customers with specific fields
            customers_flat = handler.prepare(
                "customers",
                fields=['profile.contact.email', 'profile.name']
            )
            
            # Prepare with all nested fields
            orders_flat = handler.prepare("orders")
            
            # Now use prepared DataFrames with standard operations
            result = customers_flat.join(orders_flat, ...)
        
        Note:
            This does NOT modify the original DataFrame in the handler.
            It returns a new DataFrame with extracted fields.
        """
        df_handler = self.get(name)
        
        if fields is None:
            # Extract all nested fields
            return df_handler.extract_nested_fields(verbose=verbose)
        else:
            # Validate all requested fields exist
            for path in fields:
                if path not in df_handler.extracted_fields:
                    raise ValueError(
                        f"Path '{path}' not found in nested structure. "
                        f"Available: {list(df_handler.extracted_fields.keys())}"
                    )
            
            # Extract all nested fields first
            extracted = df_handler.extract_nested_fields(verbose=verbose)
            
            # Build list of columns to keep:
            # 1. All original non-nested columns (keep regular columns)
            # 2. Only the requested nested fields (by their extracted names)
            needed_cols = []
            
            # Check which original columns are top-level nested structs
            # nested_fields has paths like "profile.contact.email", not "profile"
            # So we need to check if a column is the root of any nested path
            nested_root_columns = set()
            for nested_path in df_handler.nested_fields.keys():
                # Get the top-level column name (before first dot)
                root = nested_path.split('.')[0]
                nested_root_columns.add(root)
            
            # Add non-nested original columns (exclude nested struct columns)
            for col in df_handler.original_columns:
                if col not in nested_root_columns:
                    needed_cols.append(col)
            
            # Add requested nested fields (by their extracted names)
            for path in fields:
                extracted_name = df_handler.extracted_fields[path]
                needed_cols.append(extracted_name)
            
            # Select only the needed columns
            return DataFrame(extracted._data.select(*needed_cols))
    
    def join(
        self,
        tables: dict[str, str | list[str] | None],
        on: list[tuple[str, str, str, str]] | None = None,
        predicates: list | None = None,
        how: str = "inner"
    ) -> DataFrame:
        """
        Convenience method for SQL-like joins on multiple tables.
        
        This is syntactic sugar over prepare() + Ibis operations. It:
        1. Prepares DataFrames (extracts nested fields if specified)
        2. Builds join predicates
        3. Delegates to Ibis for execution
        4. Returns result DataFrame
        
        For complex queries (WHERE, HAVING, windows), use prepare() + direct Ibis!
        
        Args:
            tables: Dict mapping alias -> DataFrame name or list of fields to extract
                   - str value: Use DataFrame by name, extract all nested fields
                   - list value: Extract only specified nested fields
                   
                   Examples:
                   - {"c": "customers"}  # All fields from customers
                   - {"c": "customers", "o": "orders"}  # All fields from both
                   - {"c": ["profile.email", "name"]}  # Only specific fields
                   
            on: List of join conditions as (table1_alias, col1, table2_alias, col2) tuples
                Each tuple specifies: (left_table, left_column, right_table, right_column)
                
                Examples:
                - [("c", "customer_id", "o", "customer_id")]  # Single condition
                - [("c", "email", "o", "customer_email"),     # Multi-column join
                   ("c", "region", "o", "region")]
                   
            predicates: Alternative to 'on': provide raw Ibis predicates for complex conditions
                       If provided, 'on' is ignored
                       
            how: Join type - 'inner', 'left', 'right', 'outer', 'cross', 'semi', 'anti'
                All Ibis join types supported (default: 'inner')
        
        Returns:
            Joined DataFrame (leanframe.DataFrame wrapping Ibis table)
        
        Raises:
            ValueError: If tables dict is empty, aliases invalid, or join conditions malformed
            KeyError: If referenced DataFrames don't exist in handler
        
        Examples:
            # Simple two-table join
            result = handler.join(
                tables={"c": "customers", "o": "orders"},
                on=[("c", "customer_id", "o", "customer_id")],
                how="inner"
            )
            
            # Join with selective nested field extraction
            # Note: Use dot notation naturally - gets converted to underscores internally
            result = handler.join(
                tables={
                    "c": ["profile.contact.email", "profile.name"],  # Only these fields
                    "o": "orders"  # All fields
                },
                on=[("c", "profile.contact.email", "o", "customer.email")],  # Dot notation OK!
                how="left"
            )
            
            # Three-table join with multiple conditions
            result = handler.join(
                tables={"c": "customers", "o": "orders", "p": "products"},
                on=[
                    ("c", "customer_id", "o", "customer_id"),
                    ("o", "product_id", "p", "product_id")
                ],
                how="inner"
            )
            
            # Cross join (no conditions needed)
            result = handler.join(
                tables={"c": "customers", "p": "products"},
                how="cross"
            )
            
            # Continue with Ibis operations on result
            filtered = result._data.filter(result._data.amount > 100)
            grouped = filtered.group_by("region").aggregate(total=filtered.amount.sum())
            final = DataFrame(grouped)
        
        Note:
            For complex queries with WHERE, HAVING, window functions, etc.,
            use prepare() directly and compose Ibis operations:
            
                customers = handler.prepare("customers")
                orders = handler.prepare("orders")
                
                # Full Ibis power available
                result = customers._data.join(orders._data, ...).filter(...).group_by(...)
        """
        if not tables:
            raise ValueError("Must provide at least one table in 'tables' dict")
        
        if not on and not predicates and how != "cross":
            raise ValueError(
                "Must provide either 'on' conditions or 'predicates', "
                "or use how='cross' for cross join"
            )
        
        print(f"\n🔗 Joining {len(tables)} table(s) using '{how}' join...")
        
        # Step 1: Prepare all tables (extract nested fields as needed)
        prepared_tables: dict[str, DataFrame] = {}
        
        for alias, spec in tables.items():
            if isinstance(spec, str):
                # It's a DataFrame name - prepare with all fields
                print(f"   📋 Preparing '{alias}' from '{spec}' (all fields)")
                prepared_tables[alias] = self.prepare(spec, verbose=False)
            elif isinstance(spec, list):
                # It's a list of field paths - selective extraction
                # Need to extract the DataFrame name from the alias
                # For now, assume the first table entry is the reference
                # This is a limitation - might need to adjust API
                raise NotImplementedError(
                    "Selective field extraction syntax not yet implemented.\n"
                    "Use prepare() with fields parameter, then pass DataFrame name:\n"
                    "  handler.add('c_prepared', handler.prepare('customers', fields=[...]))\n"
                    "  result = handler.join(tables={'c': 'c_prepared', ...}, ...)"
                )
            else:
                raise ValueError(
                    f"Invalid table spec for alias '{alias}': {type(spec)}. "
                    f"Expected str (DataFrame name) or list (field paths)"
                )
        
        # Step 2: Build the join chain
        # Start with the first table
        aliases = list(prepared_tables.keys())
        if len(aliases) == 0:
            raise ValueError("No tables to join")
        
        # Get first table
        result_table = prepared_tables[aliases[0]]._data
        print(f"   ✅ Starting with '{aliases[0]}': {len(result_table.columns)} columns")
        
        # Join with remaining tables sequentially
        for i in range(1, len(aliases)):
            right_alias = aliases[i]
            right_table = prepared_tables[right_alias]._data
            
            print(f"   🔗 Joining with '{right_alias}': {len(right_table.columns)} columns")
            
            # Build predicates for this join
            if predicates is not None:
                # Use raw Ibis predicates (advanced usage)
                join_predicates = predicates
            elif on:
                # Build predicates from 'on' conditions
                # Filter conditions that involve the current right table
                join_predicates = []
                for condition in on:
                    if len(condition) != 4:
                        raise ValueError(
                            f"Invalid join condition: {condition}. "
                            f"Expected (table1_alias, col1, table2_alias, col2)"
                        )
                    
                    left_alias, left_col, right_alias_cond, right_col = condition
                    
                    # Check if this condition involves the current right table
                    if right_alias_cond == right_alias:
                        # Convert dot notation to underscore (user convenience)
                        # User can write "profile.contact.email" and we convert to "profile_contact_email"
                        left_col_normalized = left_col.replace(".", "_")
                        right_col_normalized = right_col.replace(".", "_")
                        
                        # Build Ibis predicate
                        # Need to access the correct table - tricky with chained joins
                        # For now, use column name directly
                        join_predicates.append(
                            (result_table[left_col_normalized], right_table[right_col_normalized])
                        )
            else:
                join_predicates = []
            
            # Perform the join
            if join_predicates:
                result_table = result_table.join(
                    right_table,
                    predicates=join_predicates,
                    how=how  # type: ignore
                )
            else:
                # Cross join (no predicates)
                result_table = result_table.join(
                    right_table,
                    how=how  # type: ignore
                )
        
        result_df = DataFrame(result_table)
        print(f"   ✅ Join complete: {len(result_df.columns)} total columns")
        
        return result_df
    
    # Legacy join methods - DEPRECATED
    # Use prepare() + direct Ibis operations instead for full SQL flexibility

    
    def __repr__(self) -> str:
        count = len(self._handlers)
        names = ", ".join(self._handlers.keys()) if count > 0 else "empty"
        return f"NestedHandler({count} DataFrames: {names})"
    
    def __len__(self) -> int:
        """Return number of managed DataFrames."""
        return len(self._handlers)
    
    def __contains__(self, name: str) -> bool:
        """Check if a DataFrame name exists in the handler."""
        return name in self._handlers
