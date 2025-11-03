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
from typing import Any, Iterator

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


"""
Dynamic Nested Data Handler for leanframe

This provides a truly dynamic handler that can introspect any DataFrame
and automatically handle nested columns of any depth and structure.
"""

# TODO: replace prints by logging, ask TIM about logger usage
class DynamicNestedHandler:
    """
    Completely dynamic nested data handler that can introspect any DataFrame
    and automatically extract nested fields of arbitrary depth.

    Features:
    - Automatic nested structure detection
    - Multi-level nesting support
    - Dynamic field extraction
    - Preserves non-nested columns
    - Dictionary-like record access
    - Efficient columnar operations
    """

    def __init__(self, lf_df: DataFrame, max_depth: int = 10):
        self.original_df = lf_df
        self.max_depth = max_depth
        self.nested_fields: dict[str, dict] = {}  # Maps original_path -> extracted_column_name
        self.struct_columns: set[str] = set()  # Track which columns are structs
        self.extracted_df = lf_df  # Initialize with original, will be replaced
        self._arrow_cache = None

        # Perform dynamic introspection
        self._introspect_structure()
        self._extract_all_nested_fields()

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

    def _extract_all_nested_fields(self):
        """Extract all discovered nested fields into a flat DataFrame."""
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

        # Create the new DataFrame
        if select_exprs:
            extracted_table = ibis_table.select(*select_exprs)
            self.extracted_df = DataFrame(extracted_table)
        else:
            self.extracted_df = self.original_df

        print(f"   Final DataFrame columns: {len(self.extracted_df.columns)} total")

    def _get_arrow_table(self):
        """Get cached PyArrow table for efficient access."""
        if self._arrow_cache is None:
            reader = self.extracted_df._data.to_pyarrow()
            self._arrow_cache = reader
        return self._arrow_cache

    # Public API - same as before but now completely dynamic

    def get_column(self, column_name: str) -> list:
        """Get entire column - works for both original and extracted columns."""
        table = self._get_arrow_table()
        if column_name not in table.column_names:
            available = ", ".join(table.column_names)
            raise KeyError(f"Column '{column_name}' not found. Available: {available}")
        return table[column_name].to_pylist()

    @property
    def columns(self) -> list[str]:
        """All available column names (original + extracted)."""
        return self._get_arrow_table().column_names

    @property
    def original_columns(self) -> list[str]:
        """Original DataFrame column names."""
        return self.original_df.columns.tolist()

    @property
    def extracted_fields(self) -> dict[str, str]:
        """Mapping of original nested paths to extracted column names."""
        return {
            path: info["extracted_name"] for path, info in self.nested_fields.items()
        }

    def get_record(self, index: int) -> dict[str, Any]:
        """Get single record as dictionary."""
        table = self._get_arrow_table()
        if index >= len(table):
            raise IndexError(f"Index {index} out of range (0-{len(table) - 1})")

        row = table.slice(index, 1)
        record = {}
        for i, col_name in enumerate(row.column_names):
            column = row.column(i)
            record[col_name] = column[0].as_py() if len(column) > 0 else None

        return record

    def get_all_records(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Get all records as list of dictionaries."""
        table = self._get_arrow_table()
        if limit and limit < len(table):
            table = table.slice(0, limit)
        return table.to_pylist()

    def filter_by(self, column_name: str, value: Any) -> "DynamicNestedHandler":
        """Filter records using pure ibis operations."""
        if column_name not in self.columns:
            raise KeyError(f"Column '{column_name}' not found")

        ibis_table = self.extracted_df._data
        filtered_table = ibis_table.filter(ibis_table[column_name] == value)
        filtered_df = DataFrame(filtered_table)

        # Create new handler with filtered data - skip introspection since structure is same
        new_handler = DynamicNestedHandler.__new__(DynamicNestedHandler)
        new_handler.original_df = filtered_df
        new_handler.extracted_df = filtered_df
        new_handler.nested_fields = self.nested_fields
        new_handler.struct_columns = self.struct_columns
        new_handler._arrow_cache = None
        new_handler.max_depth = self.max_depth

        return new_handler

    def show_structure(self):
        """Display the complete DataFrame structure analysis."""
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

        print(f"\nFinal flattened columns: {len(self.columns)}")
        for col in self.columns:
            print(f"  📊 {col}")

        table = self._get_arrow_table()
        print(f"\nRecords: {len(table)}")

    def __len__(self) -> int:
        return len(self._get_arrow_table())

    def __getitem__(self, index: int) -> dict[str, Any]:
        return self.get_record(index)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        for i in range(len(self)):
            yield self.get_record(i)

    def __repr__(self) -> str:
        return f"DynamicNestedHandler({len(self)} records, {len(self.original_columns)} → {len(self.columns)} columns)"

    # Dictionary-like interface for column access
    def get(self, key: str, default=None):
        """Dictionary-like get with default value"""
        try:
            return self.get_column(key)
        except KeyError:
            return default

    def keys(self):
        """Get all column names"""
        return self.columns

    def items(self):
        """Iterate over column names and their data"""
        for key in self.keys():
            yield key, self.get_column(key)

    def values(self):
        """Get all column data"""
        for key in self.keys():
            yield self.get_column(key)

    def __contains__(self, key: str) -> bool:
        """Check if column exists"""
        return key in self.columns
