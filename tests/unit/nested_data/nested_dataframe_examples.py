#!/usr/bin/env python3
"""
Sample code for creating simple nested dataframes using PyArrow in Python.

This module demonstrates various approaches to creating and working with nested
data structures in PyArrow, including:
- Simple struct (record) fields
- Arrays of scalars
- Arrays of structs
- Nested structs (structs within structs)
- Mixed complex types
"""

import pyarrow as pa
import pandas as pd
from typing import List, Dict, Any


def create_simple_struct_dataframe():
    """Create a simple dataframe with struct columns."""
    print("=== Simple Struct DataFrame ===")
    
    # Define schema with a struct field
    person_schema = pa.struct([
        pa.field("name", pa.string()),
        pa.field("age", pa.int64()),
        pa.field("city", pa.string())
    ])
    
    # Create data
    data = {
        "id": [1, 2, 3],
        "person": [
            {"name": "Alice", "age": 30, "city": "New York"},
            {"name": "Bob", "age": 25, "city": "London"},
            {"name": "Charlie", "age": 35, "city": "Tokyo"}
        ]
    }
    
    # Create table with explicit schema
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("person", person_schema)
    ])
    
    table = pa.Table.from_pydict(data, schema=schema)
    print("Schema:")
    print(table.schema)
    print("\nData:")
    print(table.to_pandas())
    return table


def create_array_of_structs_dataframe():
    """Create a dataframe with arrays of struct fields."""
    print("\n=== Array of Structs DataFrame ===")
    
    # Define schema for array of structs
    contact_schema = pa.struct([
        pa.field("type", pa.string()),
        pa.field("value", pa.string())
    ])
    
    data = {
        "person_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "contacts": [
            [
                {"type": "email", "value": "alice@example.com"},
                {"type": "phone", "value": "+1-555-0101"}
            ],
            [
                {"type": "email", "value": "bob@example.com"}
            ],
            [
                {"type": "phone", "value": "+1-555-0102"},
                {"type": "fax", "value": "+1-555-0103"}
            ]
        ]
    }
    
    schema = pa.schema([
        pa.field("person_id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("contacts", pa.list_(contact_schema))
    ])
    
    table = pa.Table.from_pydict(data, schema=schema)
    print("Schema:")
    print(table.schema)
    print("\nData:")
    print(table.to_pandas())
    return table


def create_nested_struct_dataframe():
    """Create a dataframe with nested struct fields (structs within structs)."""
    print("\n=== Nested Struct DataFrame ===")
    
    # Define nested address schema
    address_schema = pa.struct([
        pa.field("street", pa.string()),
        pa.field("city", pa.string()),
        pa.field("country", pa.string()),
        pa.field("coordinates", pa.struct([
            pa.field("lat", pa.float64()),
            pa.field("lng", pa.float64())
        ]))
    ])
    
    # Define person schema with nested address
    person_schema = pa.struct([
        pa.field("name", pa.string()),
        pa.field("age", pa.int64()),
        pa.field("address", address_schema)
    ])
    
    data = {
        "employee_id": [101, 102, 103],
        "department": ["Engineering", "Marketing", "Sales"],
        "employee": [
            {
                "name": "Alice Johnson",
                "age": 30,
                "address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "country": "USA",
                    "coordinates": {"lat": 40.7128, "lng": -74.0060}
                }
            },
            {
                "name": "Bob Smith",
                "age": 25,
                "address": {
                    "street": "456 Oak Ave",
                    "city": "London",
                    "country": "UK",
                    "coordinates": {"lat": 51.5074, "lng": -0.1278}
                }
            },
            {
                "name": "Charlie Brown",
                "age": 35,
                "address": {
                    "street": "789 Pine Rd",
                    "city": "Tokyo",
                    "country": "Japan",
                    "coordinates": {"lat": 35.6762, "lng": 139.6503}
                }
            }
        ]
    }
    
    schema = pa.schema([
        pa.field("employee_id", pa.int64()),
        pa.field("department", pa.string()),
        pa.field("employee", person_schema)
    ])
    
    table = pa.Table.from_pydict(data, schema=schema)
    print("Schema:")
    print(table.schema)
    print("\nData:")
    print(table.to_pandas())
    return table


def create_mixed_complex_dataframe():
    """Create a dataframe with various complex nested types."""
    print("\n=== Mixed Complex Types DataFrame ===")
    
    # Define project schema with array of strings and struct
    project_schema = pa.struct([
        pa.field("name", pa.string()),
        pa.field("version", pa.string()),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("maintainers", pa.list_(
            pa.struct([
                pa.field("name", pa.string()),
                pa.field("email", pa.string())
            ])
        )),
        pa.field("stats", pa.struct([
            pa.field("downloads", pa.int64()),
            pa.field("stars", pa.int64()),
            pa.field("last_updated", pa.date32())
        ]))
    ])
    
    data = {
        "id": [1, 2],
        "organization": ["Apache", "Python Software Foundation"],
        "project": [
            {
                "name": "Apache Arrow",
                "version": "14.0.0",
                "tags": ["data", "analytics", "columnar"],
                "maintainers": [
                    {"name": "Wes McKinney", "email": "wes@apache.org"},
                    {"name": "Antoine Pitrou", "email": "antoine@apache.org"}
                ],
                "stats": {
                    "downloads": 1000000,
                    "stars": 12500,
                    "last_updated": pd.Timestamp("2024-01-15").date()
                }
            },
            {
                "name": "Pandas",
                "version": "2.1.0",
                "tags": ["data", "analysis", "dataframe"],
                "maintainers": [
                    {"name": "Wes McKinney", "email": "wes@pandas.org"},
                    {"name": "Jeff Reback", "email": "jeff@pandas.org"}
                ],
                "stats": {
                    "downloads": 5000000,
                    "stars": 42000,
                    "last_updated": pd.Timestamp("2024-02-01").date()
                }
            }
        ]
    }
    
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("organization", pa.string()),
        pa.field("project", project_schema)
    ])
    
    table = pa.Table.from_pydict(data, schema=schema)
    print("Schema:")
    print(table.schema)
    print("\nData:")
    df = table.to_pandas()
    print(df)
    return table


def demonstrate_accessing_nested_data():
    """Demonstrate how to access nested data in PyArrow tables."""
    print("\n=== Accessing Nested Data ===")
    
    # Create a simple nested table
    table = create_simple_struct_dataframe()
    
    print("\n1. Converting to pandas and accessing struct fields:")
    df = table.to_pandas(types_mapper=pd.ArrowDtype)
    
    # Access struct fields in pandas (requires ArrowDtype for struct accessor)
    print("Names:", df['person'].struct.field('name').tolist())
    print("Ages:", df['person'].struct.field('age').tolist())
    print("Cities:", df['person'].struct.field('city').tolist())
    
    print("\n2. Using PyArrow field access:")
    
    # Extract struct fields using direct field access
    # Alternative approach: use field names directly
    person_array = table['person']
    
    # Method 1: Convert to pandas and use struct accessor (most reliable)
    df_temp = pa.table([person_array], names=['person']).to_pandas(types_mapper=pd.ArrowDtype)
    names_list = df_temp['person'].struct.field('name').tolist()
    ages_list = df_temp['person'].struct.field('age').tolist() 
    cities_list = df_temp['person'].struct.field('city').tolist()
    
    print("Names (via pandas struct accessor):", names_list)
    print("Ages (via pandas struct accessor):", ages_list)
    print("Cities (via pandas struct accessor):", cities_list)
    
    print("\n3. Flattening nested structure:")
    flattened_data = {
        'id': table['id'].to_pylist(),
        'person_name': names_list,
        'person_age': ages_list,
        'person_city': cities_list
    }
    flattened_df = pd.DataFrame(flattened_data)
    print("Flattened DataFrame:")
    print(flattened_df)


def create_from_json_like_data():
    """Create nested dataframes from JSON-like data structures."""
    print("\n=== Creating from JSON-like Data ===")
    
    # Simulate loading from JSON
    json_data = [
        {
            "order_id": 1001,
            "customer": {
                "id": 123,
                "name": "John Doe",
                "preferences": {
                    "newsletter": True,
                    "categories": ["electronics", "books"]
                }
            },
            "items": [
                {"product": "Laptop", "price": 999.99, "quantity": 1},
                {"product": "Mouse", "price": 29.99, "quantity": 2}
            ],
            "total": 1059.97
        },
        {
            "order_id": 1002,
            "customer": {
                "id": 456,
                "name": "Jane Smith",
                "preferences": {
                    "newsletter": False,
                    "categories": ["clothing", "accessories"]
                }
            },
            "items": [
                {"product": "Dress", "price": 79.99, "quantity": 1},
                {"product": "Shoes", "price": 129.99, "quantity": 1}
            ],
            "total": 209.98
        }
    ]
    
    # Define schema
    preferences_schema = pa.struct([
        pa.field("newsletter", pa.bool_()),
        pa.field("categories", pa.list_(pa.string()))
    ])
    
    customer_schema = pa.struct([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("preferences", preferences_schema)
    ])
    
    item_schema = pa.struct([
        pa.field("product", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("quantity", pa.int64())
    ])
    
    schema = pa.schema([
        pa.field("order_id", pa.int64()),
        pa.field("customer", customer_schema),
        pa.field("items", pa.list_(item_schema)),
        pa.field("total", pa.float64())
    ])
    
    # Convert to PyArrow table
    table = pa.Table.from_pylist(json_data, schema=schema)
    print("Schema:")
    print(table.schema)
    print("\nData:")
    print(table.to_pandas())
    return table


def working_with_pandas_extension_types():
    """Demonstrate working with pandas ArrowDtype extension types."""
    print("\n=== Working with pandas ArrowDtype ===")
    
    # Create nested data using pandas ArrowDtype directly
    person_type = pd.ArrowDtype(pa.struct([
        pa.field("name", pa.string()),
        pa.field("age", pa.int64()),
        pa.field("skills", pa.list_(pa.string()))
    ]))
    
    data = {
        "employee_id": pd.Series([1, 2, 3], dtype=pd.ArrowDtype(pa.int64())),
        "person": pd.Series([
            {"name": "Alice", "age": 30, "skills": ["Python", "SQL", "Docker"]},
            {"name": "Bob", "age": 25, "skills": ["JavaScript", "React", "Node.js"]},
            {"name": "Charlie", "age": 35, "skills": ["Java", "Spring", "Kubernetes"]}
        ], dtype=person_type),
        "salary": pd.Series([75000.0, 65000.0, 85000.0], dtype=pd.ArrowDtype(pa.float64()))
    }
    
    df = pd.DataFrame(data)
    print("DataFrame with ArrowDtype:")
    print(df)
    print("\nDtypes:")
    print(df.dtypes)
    
    # Access nested fields
    print("\nAccessing nested 'name' field:")
    print(df['person'].struct.field('name'))
    
    print("\nAccessing nested 'skills' array:")
    skills_series = df['person'].struct.field('skills')
    print(skills_series)
    
    # Convert back to PyArrow table
    table = pa.Table.from_pandas(df)
    print("\nConverted back to PyArrow table schema:")
    print(table.schema)


if __name__ == "__main__":
    print("PyArrow Nested DataFrame Examples")
    print("=" * 50)
    
    # Run all examples
    create_simple_struct_dataframe()
    create_array_of_structs_dataframe()
    create_nested_struct_dataframe()
    create_mixed_complex_dataframe()
    demonstrate_accessing_nested_data()
    create_from_json_like_data()
    working_with_pandas_extension_types()
    
    print("\n" + "=" * 50)
    print("All examples completed!")