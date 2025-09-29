#!/usr/bin/env python3
"""
Centralized nested data creation utilities for leanframe tests.

This module provides reusable functions to create nested DataFrames with
various structures to avoid code repetition across test files.
"""

from typing import Dict, Any
import pyarrow as pa
import pandas as pd
import ibis
import leanframe
from leanframe.core.frame import DataFrame


# Common schema definitions
def get_person_schema() -> pa.StructType:
    """Standard person struct schema."""
    return pa.struct([
        pa.field("name", pa.string()),
        pa.field("age", pa.int64()),
        pa.field("city", pa.string())
    ])


def get_contact_schema() -> pa.StructType:
    """Standard contact struct schema."""
    return pa.struct([
        pa.field("email", pa.string()),
        pa.field("phone", pa.string())
    ])


def get_address_schema() -> pa.StructType:
    """Standard address struct schema."""
    return pa.struct([
        pa.field("street", pa.string()),
        pa.field("zip", pa.string()),
        pa.field("country", pa.string())
    ])


def create_basic_person_contact_data(num_records: int = 3) -> Dict[str, Any]:
    """Create basic nested data with person and contact structures."""
    
    # Base data that can be extended
    base_people = [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": 25, "city": "Los Angeles"}, 
        {"name": "Charlie", "age": 35, "city": "Chicago"},
        {"name": "Diana", "age": 28, "city": "Miami"},
        {"name": "Eve", "age": 32, "city": "Seattle"}
    ]
    
    base_contacts = [
        {"email": "alice@example.com", "phone": "555-1234"},
        {"email": "bob@example.com", "phone": "555-5678"},
        {"email": "charlie@example.com", "phone": "555-9012"}, 
        {"email": "diana@example.com", "phone": "555-3456"},
        {"email": "eve@example.com", "phone": "555-7890"}
    ]
    
    # Take only the requested number of records
    people = base_people[:num_records]
    contacts = base_contacts[:num_records]
    ids = list(range(1, num_records + 1))
    
    return {
        "id": ids,
        "person": people,
        "contact": contacts
    }


def create_extended_data_with_address(num_records: int = 4) -> Dict[str, Any]:
    """Create extended data with person, contact, and address structures."""
    
    # Get basic data
    data = create_basic_person_contact_data(num_records)
    
    # Add addresses
    base_addresses = [
        {"street": "123 Main St", "zip": "10001", "country": "USA"},
        {"street": "456 Oak Ave", "zip": "90210", "country": "USA"},
        {"street": "789 Pine Rd", "zip": "60601", "country": "USA"},
        {"street": "321 Beach Blvd", "zip": "33139", "country": "USA"},
        {"street": "654 Hill Dr", "zip": "98101", "country": "USA"}
    ]
    
    data["address"] = base_addresses[:num_records]
    return data


def pyarrow_to_leanframe(pa_table: pa.Table) -> DataFrame:
    """
    Convert PyArrow table to leanframe DataFrame using the proven pattern.
    
    Uses minimal pandas only for the conversion, following create_data.py pattern.
    """
    # Minimal pandas usage for conversion
    df_pd = pa_table.to_pandas(types_mapper=pd.ArrowDtype)
    
    # Create leanframe DataFrame using proven pattern
    backend = ibis.duckdb.connect()
    session = leanframe.Session(backend=backend)
    df_lf = session.DataFrame(df_pd)
    
    # Clean up pandas reference
    del df_pd
    
    return df_lf


def create_simple_nested_dataframe(num_records: int = 3) -> DataFrame:
    """Create simple nested DataFrame with person and contact."""
    
    # Create data and schema
    data = create_basic_person_contact_data(num_records)
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("person", get_person_schema()),
        pa.field("contact", get_contact_schema())
    ])
    
    # Create PyArrow table and convert to leanframe
    pa_table = pa.Table.from_pydict(data, schema=schema)
    return pyarrow_to_leanframe(pa_table)


def create_extended_nested_dataframe(num_records: int = 4) -> DataFrame:
    """Create extended nested DataFrame with person, contact, and address."""
    
    # Create data and schema
    data = create_extended_data_with_address(num_records)
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("person", get_person_schema()),
        pa.field("contact", get_contact_schema()),
        pa.field("address", get_address_schema())
    ])
    
    # Create PyArrow table and convert to leanframe
    pa_table = pa.Table.from_pydict(data, schema=schema)
    return pyarrow_to_leanframe(pa_table)


def create_array_of_structs_dataframe() -> DataFrame:
    """Create DataFrame with array of structs (for advanced examples)."""
    
    data = {
        "person_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "contacts": [
            [
                {"type": "email", "value": "alice@work.com"},
                {"type": "phone", "value": "555-0001"}
            ],
            [
                {"type": "email", "value": "bob@work.com"},
                {"type": "phone", "value": "555-0002"},
                {"type": "slack", "value": "@bob"}
            ],
            [
                {"type": "email", "value": "charlie@work.com"}
            ]
        ]
    }
    
    contact_schema = pa.struct([
        pa.field("type", pa.string()),
        pa.field("value", pa.string())
    ])
    
    schema = pa.schema([
        pa.field("person_id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("contacts", pa.list_(contact_schema))
    ])
    
    pa_table = pa.Table.from_pydict(data, schema=schema)
    return pyarrow_to_leanframe(pa_table)


def create_deeply_nested_dataframe() -> DataFrame:
    """Create DataFrame with deeply nested structures."""
    
    data = {
        "id": [1, 2],
        "employee": [
            {
                "name": "Alice",
                "details": {
                    "age": 30,
                    "address": {
                        "street": "123 Main St",
                        "coordinates": {"lat": 40.7128, "lon": -74.0060}
                    }
                }
            },
            {
                "name": "Bob", 
                "details": {
                    "age": 25,
                    "address": {
                        "street": "456 Oak Ave",
                        "coordinates": {"lat": 34.0522, "lon": -118.2437}
                    }
                }
            }
        ]
    }
    
    # Deep nesting schema
    coordinates_schema = pa.struct([
        pa.field("lat", pa.float64()),
        pa.field("lon", pa.float64())
    ])
    
    address_schema = pa.struct([
        pa.field("street", pa.string()),
        pa.field("coordinates", coordinates_schema)
    ])
    
    details_schema = pa.struct([
        pa.field("age", pa.int64()),
        pa.field("address", address_schema)
    ])
    
    employee_schema = pa.struct([
        pa.field("name", pa.string()),
        pa.field("details", details_schema)
    ])
    
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("employee", employee_schema)
    ])
    
    pa_table = pa.Table.from_pydict(data, schema=schema)
    return pyarrow_to_leanframe(pa_table)


# Convenience functions for backward compatibility
if __name__ == "__main__":
    print("=== Centralized Nested Data Creation Demo ===\n")
    
    # Test different creation functions
    print("1. Simple nested DataFrame (3 records):")
    simple_df = create_simple_nested_dataframe(3)
    print(f"   Columns: {simple_df.columns.tolist()}")
    
    print("\n2. Extended nested DataFrame (4 records):")
    extended_df = create_extended_nested_dataframe(4)
    print(f"   Columns: {extended_df.columns.tolist()}")
    
    print("\n3. Array of structs DataFrame:")
    array_df = create_array_of_structs_dataframe()
    print(f"   Columns: {array_df.columns.tolist()}")
    
    print("\n4. Deeply nested DataFrame:")
    deep_df = create_deeply_nested_dataframe()
    print(f"   Columns: {deep_df.columns.tolist()}")
    
    print("\n✅ All nested data creation functions working!")