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

"""DataFrame is a two dimensional data structure."""

from __future__ import annotations

import ibis
import ibis.expr.types as ibis_types
import pandas as pd

from leanframe.core.dtypes import convert_ibis_to_pandas


class DataFrame:
    """A 2D data structure, representing data and deferred computation.

    WARNING: Do not call this constructor directly. Use the factory methods on
    Session, instead.
    """

    def __init__(self, data: ibis_types.Table):
        self._data = data

    @property
    def columns(self) -> pd.Index:
        """The column labels of the DataFrame."""
        return pd.Index(self._data.columns, dtype="object")

    @property
    def dtypes(self) -> pd.Series:
        """Return the dtypes in the DataFrame."""
        names = self._data.columns
        types = [convert_ibis_to_pandas(t) for t in self._data.schema().types]
        return pd.Series(types, index=names, name="dtypes")

    def __getitem__(self, key: str):
        """Get a column.

        Note: direct row access via an Index is intentionally not implemented by
        leanframe. Check out a project like Google's BigQuery DataFrames
        (bigframes) if you require indexing.
        """
        import leanframe.core.series

        # TODO(tswast): Support filtering by a boolean Series if we get a Series
        # instead of a key? If so, the Series would have to be a column of the
        # current DataFrame, only. No joins by index key are available.
        return leanframe.core.series.Series(self._data[key])

    def assign(self, **kwargs):
        """Assign new columns to a DataFrame.

        This corresponds to the select() method in Ibis.

        Args:
            kwargs:
                The column names are keywords. If the values are not callable,
                (e.g. a Series, scalar, or array), they are simply assigned.
        """
        named_exprs = {name: self._data[name] for name in self._data.columns}
        new_exprs = {}
        for name, value in kwargs.items():
            expr = getattr(value, "_data", None)
            if expr is None:
                expr = ibis.literal(value)
            new_exprs[name] = expr

        named_exprs.update(new_exprs)
        return DataFrame(self._data.select(**named_exprs))

    def to_pandas(self) -> pd.DataFrame:
        """Convert the DataFrame to a pandas.DataFrame.

        Where possible, pandas.ArrowDtype is used to avoid lossy conversions
        from the database types to pandas.
        """
        return self._data.to_pyarrow().to_pandas(
            types_mapper=lambda type_: pd.ArrowDtype(type_)
        )

    def to_ibis(self) -> ibis_types.Table:
        """Return the underlying Ibis expression."""
        return self._data


"""
Dynamic Nested Data Handler for leanframe

This provides a truly dynamic handler that can introspect any DataFrame
and automatically handle nested columns of any depth and structure.
"""

# TODO: replace prints by logging, ask TIM about logger usage
class DataFrameHandler:
    """
    Wrapper for a single leanframe DataFrame that handles nested column introspection.
    
    This class wraps ONE DataFrame and provides metadata about its nested structure
    along with functional operations for extracting nested fields.
    
    Design Philosophy - Stateless Data, Cached Metadata:
    - Wraps a SINGLE leanframe DataFrame (not multiple DataFrames)
    - Caches IMMUTABLE schema metadata (nested field mappings, column types)
    - Does NOT cache data or extraction results
    - All data operations return NEW DataFrame objects (functional style)
    - Thread-safe for concurrent operations
    
    Features:
    - Automatic nested structure detection via schema introspection
    - Multi-level nesting support (configurable depth)
    - Dynamic field extraction (computed on-demand)
    - Preserves non-nested columns
    - Efficient columnar operations
    
    Usage Pattern:
        # Create handler for a single DataFrame (introspects schema once)
        handler = DataFrameHandler(customers_df)
        
        # Access cached schema metadata (fast, immutable)
        print(handler.extracted_fields)  # {'person.name': 'person_name', ...}
        handler.show_structure()
        
        # Compute data operations (functional, returns new objects)
        extracted_df = handler.extract_nested_fields()  # No caching!
        another_df = handler.extract_nested_fields()    # Computes again
        
        # For multi-DataFrame operations, use NestedHandler orchestrator
        # (See NestedHandler class for joins and other cross-DataFrame operations)
    """

    def __init__(
        self, 
        lf_df: DataFrame, 
        max_depth: int = 10,
        table_qualifier: str | None = None
    ):
        """
        Initialize handler by introspecting DataFrame schema.
        
        Args:
            lf_df: leanframe DataFrame to analyze
            max_depth: Maximum nesting depth to analyze (prevents infinite recursion)
            table_qualifier: Optional backend table identifier (e.g., "project.dataset.table")
                           Can be None for in-memory DataFrames. Can be updated later
                           via set_table_qualifier() method.
            
        Note:
            Constructor only analyzes SCHEMA (metadata), does not extract data.
            This makes handler creation fast and allows metadata reuse.
        """
        # Store reference to original DataFrame (not a copy!)
        self.original_df = lf_df
        
        # Configuration
        self.max_depth = max_depth
        
        # Backend reference - can be None for in-memory DataFrames
        # This is mutable and can be updated when DataFrame is saved to backend
        self._table_qualifier: str | None = table_qualifier
        
        # Cached schema metadata (immutable after introspection)
        self.nested_fields: dict[str, dict] = {}  # Maps path -> field metadata
        self.struct_columns: set[str] = set()  # Tracks struct columns
        
        # Perform schema introspection (builds metadata cache)
        self._introspect_structure()

    def _introspect_structure(self):
        """Dynamically introspect the DataFrame to find all nested structures."""

        # Get underlying ibis table and its schema
        ibis_table = self.original_df._data
        schema = ibis_table.schema()

        # Analyze each column using proper ibis schema introspection
        for column_name in ibis_table.columns:
            try:
                column_type = schema[column_name]
                print(f"   📋 Column '{column_name}': {column_type}")

                # Use ibis native type checking instead of string parsing
                if column_type.is_struct():
                    self.struct_columns.add(column_name)
                    self._analyze_struct_column_native(
                        ibis_table[column_name], column_name, column_name, depth=0
                    )
                else:
                    print("      → Regular column, keeping as-is")

            except Exception as e:
                print(f"   ⚠️  Could not analyze column '{column_name}': {e}")

    def _analyze_struct_column_native(
        self, struct_expr, field_path: str, root_column: str, depth: int
    ):
        """Use native ibis introspection instead of string parsing"""
        if depth >= self.max_depth:
            return

        indent = "    " * (depth + 1)
        print(f"{indent}🏗️  Analyzing struct '{field_path}' using native ibis")

        # Get the struct type directly from ibis
        struct_type = struct_expr.type()

        # Iterate through fields using ibis native field access
        if hasattr(struct_type, "fields"):
            print(f"{indent}   Found {len(struct_type.fields)} fields in struct")

            for field_name, field_type in struct_type.fields.items():
                current_path = f"{field_path}.{field_name}"
                extracted_name = (
                    f"{root_column}_{field_name}"
                    if depth == 0
                    else f"{field_path.replace('.', '_')}_{field_name}"
                )

                print(f"{indent}  📝 Field: {field_name} ({field_type})")

                try:
                    # Access the field directly using ibis
                    field_expr = struct_expr[field_name]

                    if field_type.is_struct():
                        print(f"{indent}    🏗️  Nested struct, recursing...")
                        self._analyze_struct_column_native(
                            field_expr, current_path, root_column, depth + 1
                        )
                    else:
                        # This is a leaf field we can extract
                        print(
                            f"{indent}    ✅ Extractable: {current_path} → {extracted_name}"
                        )
                        self.nested_fields[current_path] = {
                            "expression": field_expr,
                            "extracted_name": extracted_name,
                            "original_path": current_path,
                            "type": str(field_type),
                        }

                except Exception as e:
                    print(f"{indent}    ❌ Cannot access field {field_name}: {e}")
        else:
            print(f"{indent}   ⚠️  Struct type has no accessible fields attribute")

    def _extract_nested_fields_silent(self) -> DataFrame:
        """
        Internal method: Extract nested fields without printing.
        Used by backward compatibility methods.
        """
        ibis_table = self.original_df._data
        select_exprs = []

        # Add all non-struct columns first
        for col_name in ibis_table.columns:
            if col_name not in self.struct_columns:
                select_exprs.append(ibis_table[col_name])

        # Add all extracted nested fields
        for field_path, field_info in self.nested_fields.items():
            try:
                field_expr = field_info["expression"]
                extracted_name = field_info["extracted_name"]
                extraction_expr = field_expr.name(extracted_name)
                select_exprs.append(extraction_expr)
            except Exception:
                pass  # Skip fields that can't be extracted

        # Create and return the new DataFrame
        if select_exprs:
            extracted_table = ibis_table.select(*select_exprs)
            return DataFrame(extracted_table)
        else:
            return self.original_df

    def extract_nested_fields(self, verbose: bool = True) -> DataFrame:
        """
        Extract all discovered nested fields into a flat DataFrame.
        
        IMPORTANT: This is a FUNCTIONAL operation that returns a NEW DataFrame.
        Results are NOT cached - each call computes fresh extraction.
        This ensures thread-safety and prevents stale data issues.
        
        Args:
            verbose: If True, prints extraction progress. Set False for silent operation.
        
        Returns:
            NEW DataFrame with flattened structure (does not modify original)
            
        Example:
            handler = DynamicNestedHandler(nested_df)
            flat1 = handler.extract_nested_fields()  # Computes extraction
            flat2 = handler.extract_nested_fields()  # Computes again (no cache!)
        """
        if not verbose:
            return self._extract_nested_fields_silent()
            
        print("\n🚀 Extracting all nested fields...")

        ibis_table = self.original_df._data
        select_exprs = []

        # Add all non-struct columns first
        for col_name in ibis_table.columns:
            if col_name not in self.struct_columns:
                select_exprs.append(ibis_table[col_name])
                print(f"   ✅ Keeping regular column: {col_name}")

        # Add all extracted nested fields
        extracted_count = 0
        for field_path, field_info in self.nested_fields.items():
            try:
                # Use the pre-built expression from native introspection
                field_expr = field_info["expression"]
                extracted_name = field_info["extracted_name"]

                # Create the extraction expression with proper naming
                extraction_expr = field_expr.name(extracted_name)
                select_exprs.append(extraction_expr)

                print(f"   ✅ Extracted: {field_path} → {extracted_name}")
                extracted_count += 1

            except Exception as e:
                print(f"   ❌ Failed to extract {field_path}: {e}")

        print(f"\n📊 Summary: {extracted_count} nested fields extracted")

        # Create and return the new DataFrame (no state storage!)
        if select_exprs:
            extracted_table = ibis_table.select(*select_exprs)
            result = DataFrame(extracted_table)
        else:
            result = self.original_df

        print(f"   Final DataFrame columns: {len(result.columns)} total")
        return result

    # Public API - Functional design without state

    @property
    def original_columns(self) -> list[str]:
        """
        Get original DataFrame column names.
        
        Returns:
            List of column names from the original DataFrame
        """
        return self.original_df.columns.tolist()

    @property
    def extracted_fields(self) -> dict[str, str]:
        """
        Get mapping of nested paths to extracted column names.
        
        This returns CACHED SCHEMA METADATA, not data.
        Example: {'person.name': 'person_name', 'person.age': 'person_age'}
        
        Returns:
            Dictionary mapping nested paths to flattened column names
        """
        return {
            path: info["extracted_name"] for path, info in self.nested_fields.items()
        }
    
    def get_extracted_column_name(self, nested_path: str) -> str | None:
        """
        Look up the extracted column name for a nested path.
        
        Args:
            nested_path: Nested path like 'person.address.city'
            
        Returns:
            Extracted column name like 'person_address_city', or None if not found
        """
        return self.extracted_fields.get(nested_path)

    # Backward compatibility methods - compute on-demand (NO caching!)
    @property
    def columns(self) -> list[str]:
        """
        Get column names from extracted DataFrame (computed on-demand).
        
        WARNING: This performs extraction on EVERY call - use sparingly.
        For efficiency, call extract_nested_fields() once and reuse the result.
        """
        extracted = self._extract_nested_fields_silent()
        return extracted.columns.tolist()
    
    def get_column(self, column_name: str) -> list:
        """
        Get entire column data (computed on-demand, not cached).
        
        WARNING: This extracts and materializes data on EVERY call.
        For efficiency, call extract_nested_fields() once and work with that.
        """
        extracted = self._extract_nested_fields_silent()
        pandas_df = extracted.to_pandas()
        if column_name not in pandas_df.columns:
            available = ", ".join(pandas_df.columns)
            raise KeyError(f"Column '{column_name}' not found. Available: {available}")
        return pandas_df[column_name].tolist()
    
    def get_record(self, index: int) -> dict:
        """
        Get single record as dictionary (computed on-demand).
        
        WARNING: Performs extraction and materialization on EVERY call.
        Not efficient for iterating over records - use extract_nested_fields() instead.
        """
        extracted = self._extract_nested_fields_silent()
        pandas_df = extracted.to_pandas()
        if index >= len(pandas_df):
            raise IndexError(f"Index {index} out of range (0-{len(pandas_df) - 1})")
        return pandas_df.iloc[index].to_dict()
    
    def __len__(self) -> int:
        """
        Get number of records from original DataFrame.
        
        Note: Uses original DataFrame to avoid extraction overhead.
        """
        # Use original DataFrame to avoid extraction overhead
        pandas_df = self.original_df.to_pandas()
        return len(pandas_df)
    
    def __getitem__(self, index: int) -> dict:
        """Get record by index."""
        return self.get_record(index)
    
    def __iter__(self):
        """Iterate over records."""
        for i in range(len(self)):
            yield self.get_record(i)
    
    def __contains__(self, key: str) -> bool:
        """Check if column exists in extracted fields."""
        return key in self.columns
    
    def keys(self):
        """Get all column names."""
        return self.columns
    
    def items(self):
        """Iterate over (column_name, column_data) pairs."""
        for key in self.keys():
            yield key, self.get_column(key)
    
    def values(self):
        """Iterate over column data."""
        for key in self.keys():
            yield self.get_column(key)
    
    def get(self, key: str, default=None):
        """Dictionary-like get with default value."""
        try:
            return self.get_column(key)
        except KeyError:
            return default

    def show_structure(self):
        """
        Display the complete DataFrame structure analysis.
        
        Shows CACHED SCHEMA METADATA:
        - Original columns and their types
        - Discovered nested fields and their paths
        - Expected flattened column structure
        - Record count from original DataFrame
        
        This is a read-only view of cached metadata, not data extraction.
        """
        print("\n📋 DYNAMIC STRUCTURE ANALYSIS")
        print("=" * 50)

        print(f"Original columns: {len(self.original_columns)}")
        for col in self.original_columns:
            if col in self.struct_columns:
                print(f"  🏗️  {col} (struct - nested)")
            else:
                print(f"  📄 {col} (regular)")

        print(f"\nExtracted nested fields: {len(self.nested_fields)}")
        for original_path, extracted_name in self.extracted_fields.items():
            print(f"  🔗 {original_path} → {extracted_name}")

        # Show what the flattened structure would look like
        expected_columns = [col for col in self.original_columns if col not in self.struct_columns]
        expected_columns.extend(self.extracted_fields.values())
        print(f"\nFlattened columns ({len(expected_columns)} total):")
        for col in expected_columns:
            print(f"  📊 {col}")

        # Show record count from original
        pandas_preview = self.original_df.to_pandas()
        print(f"\nRecords: {len(pandas_preview)}")

    # Backend reference management
    
    @property
    def table_qualifier(self) -> str | None:
        """
        Get the backend table qualifier for this DataFrame.
        
        Returns:
            Backend table identifier (e.g., "project.dataset.table") or None
            if this is an in-memory DataFrame not yet persisted to backend.
        
        Example:
            handler = DataFrameHandler(df)
            print(handler.table_qualifier)  # None (in-memory)
            
            # After saving to backend
            handler.set_table_qualifier("mydb.sales.customers")
            print(handler.table_qualifier)  # "mydb.sales.customers"
        """
        return self._table_qualifier
    
    def set_table_qualifier(self, qualifier: str | None):
        """
        Update the backend table qualifier for this DataFrame.
        
        This should be called when:
        - DataFrame is saved to a backend table
        - DataFrame is loaded from a backend table
        - Backend table is dropped (set to None)
        - DataFrame is renamed in the backend
        
        Args:
            qualifier: New backend table identifier or None to clear
        
        Example:
            handler = DataFrameHandler(in_memory_df)
            
            # Save to backend
            backend.create_table("customers", df.to_pandas())
            handler.set_table_qualifier("mydb.main.customers")
            
            # Later, if table is dropped
            backend.drop_table("customers")
            handler.set_table_qualifier(None)  # DataFrame still in memory
        """
        old_qualifier = self._table_qualifier
        self._table_qualifier = qualifier
        
        if old_qualifier != qualifier:
            if qualifier is None:
                print(f"🔗 Cleared backend reference (was: {old_qualifier})")
            elif old_qualifier is None:
                print(f"🔗 Set backend reference: {qualifier}")
            else:
                print(f"🔗 Updated backend reference: {old_qualifier} → {qualifier}")
    
    def has_backend_table(self) -> bool:
        """
        Check if this DataFrame has a backend table reference.
        
        Returns:
            True if DataFrame has a backend table, False if in-memory only
        """
        return self._table_qualifier is not None
    
    def get_backend_info(self) -> dict[str, str | None]:
        """
        Get information about the backend storage.
        
        Returns:
            Dictionary with backend information:
            - 'qualifier': Full table qualifier or None
            - 'project': Project name (if qualifier follows project.dataset.table pattern)
            - 'dataset': Dataset name (if applicable)
            - 'table': Table name (if applicable)
            - 'type': 'backend' or 'memory'
        
        Example:
            info = handler.get_backend_info()
            # {'qualifier': 'myproject.sales.customers',
            #  'project': 'myproject',
            #  'dataset': 'sales',
            #  'table': 'customers',
            #  'type': 'backend'}
        """
        if self._table_qualifier is None:
            return {
                'qualifier': None,
                'project': None,
                'dataset': None,
                'table': None,
                'type': 'memory'
            }
        
        # Try to parse standard format: project.dataset.table
        parts = self._table_qualifier.split('.')
        info: dict[str, str | None] = {
            'qualifier': self._table_qualifier,
            'project': None,
            'dataset': None,
            'table': None,
            'type': 'backend'
        }
        
        if len(parts) == 3:
            info['project'] = parts[0]
            info['dataset'] = parts[1]
            info['table'] = parts[2]
        elif len(parts) == 2:
            info['dataset'] = parts[0]
            info['table'] = parts[1]
        elif len(parts) == 1:
            info['table'] = parts[0]
        
        return info

    def __repr__(self) -> str:
        num_extracted = len(self.nested_fields)
        backend_info = " [in-memory]" if not self.has_backend_table() else f" [{self._table_qualifier}]"
        return f"DataFrameHandler({len(self.original_columns)} cols → {num_extracted} nested fields{backend_info})"
