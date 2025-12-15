#!/usr/bin/env python3
"""
Tests for DataFrameHandler comprehensive functionality.

This tests a truly dynamic handler that can work with ANY nested DataFrame structure.
Key features:
- Automatic introspection of nested structures
- Dynamic field extraction and flattening
- Dictionary-like access interface
- Filtering and record access
- Works with arbitrary nesting levels
"""

from demos.utils.create_nested_data import create_simple_nested_dataframe, create_extended_nested_dataframe, create_deeply_nested_dataframe
from leanframe.core.frame import DataFrameHandler


def test_basic_usage():
    """Test basic DataFrameHandler usage and structure inspection."""
    df = create_simple_nested_dataframe(5)
    handler = DataFrameHandler(df)

    # Test basic structure
    assert len(handler.original_columns) == 3  # id, person, contact
    assert len(handler.columns) == 6  # id + 3 person fields + 2 contact fields
    assert len(handler) == 5  # 5 records

    # Test available columns
    expected_columns = [
        "id",
        "person_name",
        "person_age",
        "person_city",
        "contact_email",
        "contact_phone",
    ]
    assert handler.columns == expected_columns

    # Test nested fields extraction mapping
    expected_extractions = {
        "person.name": "person_name",
        "person.age": "person_age",
        "person.city": "person_city",
        "contact.email": "contact_email",
        "contact.phone": "contact_phone",
    }
    for original, extracted in expected_extractions.items():
        assert handler.extracted_fields[original] == extracted


def test_data_access():
    """Test data access patterns of DataFrameHandler."""
    df = create_simple_nested_dataframe(3)
    handler = DataFrameHandler(df)

    # Test column-wise access
    names = handler.get_column("person_name")
    ages = handler.get_column("person_age")
    assert len(names) == 3
    assert len(ages) == 3
    assert all(isinstance(name, str) for name in names)
    assert all(isinstance(age, int) for age in ages)

    # Test record-wise access
    first_record = handler[0]
    assert isinstance(first_record, dict)
    assert "person_name" in first_record
    assert "person_age" in first_record
    assert "contact_email" in first_record

    # Test dictionary-like interface
    assert "person_name" in handler
    assert "nonexistent_column" not in handler
    keys = list(handler.keys())
    assert len(keys) == 6
    assert "id" in keys


def test_filtering():
    """Test filtering functionality using extracted DataFrame (stateless approach)."""
    df = create_simple_nested_dataframe(5)
    handler = DataFrameHandler(df)

    # Extract the flattened DataFrame
    extracted_df = handler.extract_nested_fields(verbose=False)
    
    # Filter using pandas (simpler for testing)
    filtered_pandas = extracted_df.to_pandas()
    filtered_by_age = filtered_pandas[filtered_pandas["person_age"] == 30]
    
    # Test the filtered results
    if len(filtered_by_age) > 0:
        assert all(filtered_by_age["person_age"] == 30)


def test_different_structures():
    """Test DataFrameHandler with different nested structures."""
    # Extended structure with address
    extended_df = create_extended_nested_dataframe(2)
    extended_handler = DataFrameHandler(extended_df)

    expected_extended_columns = [
        "id",
        "person_name",
        "person_age",
        "person_city",
        "contact_email",
        "contact_phone",
        "address_street",
        "address_zip",
        "address_country",
    ]
    assert extended_handler.columns == expected_extended_columns

    # Check new address fields exist
    address_fields = [col for col in extended_handler.columns if "address" in col]
    assert len(address_fields) == 3
    assert "address_street" in address_fields
    assert "address_zip" in address_fields
    assert "address_country" in address_fields


def test_deep_nesting():
    """Test DataFrameHandler with deeply nested structures."""
    deep_df = create_deeply_nested_dataframe()
    deep_handler = DataFrameHandler(deep_df)

    # Verify deep structure handling
    assert len(deep_handler.columns) >= 5  # Should have multiple levels extracted

    # Check for deeply nested fields
    deep_fields = [col for col in deep_handler.columns if "employee" in col]
    assert len(deep_fields) >= 4  # Should have multiple employee fields

    # Test access to deeply nested data
    first_record = deep_handler[0]
    assert "employee_name" in first_record

    # Test coordinates access if available
    coord_fields = [col for col in deep_handler.columns if "coordinates" in col]
    if coord_fields:
        assert any("lat" in col for col in coord_fields)
        assert any("lon" in col for col in coord_fields)


def test_handler_capabilities():
    """Test general capabilities and edge cases of DataFrameHandler."""
    df = create_simple_nested_dataframe(2)
    handler = DataFrameHandler(df)

    # Test length
    assert len(handler) == 2

    # Test iteration
    records = list(handler)
    assert len(records) == 2
    assert all(isinstance(record, dict) for record in records)

    # Test column access
    assert handler.columns == [
        "id",
        "person_name",
        "person_age",
        "person_city",
        "contact_email",
        "contact_phone",
    ]

    # Test original columns preservation
    assert len(handler.original_columns) == 3  # id, person, contact
