#!/usr/bin/env python3
"""Test the DynamicNestedHandler from its proper location in leanframe.core"""

from leanframe.core.nested_handler import DynamicNestedHandler
from tests.unit.nested_data.create_nested_data import (
    create_simple_nested_dataframe,
    create_extended_nested_dataframe,
    create_deeply_nested_dataframe,
)


def test_simple_nested_structure():
    """Test dynamic handler with simple nested structure (person + contact)."""
    simple_df = create_simple_nested_dataframe(3)
    handler = DynamicNestedHandler(simple_df)

    # Test basic properties
    assert len(handler) == 3
    assert len(handler.original_columns) == 3  # id, person, contact
    assert len(handler.columns) == 6  # flattened fields

    # Test record access
    record = handler[0]
    assert isinstance(record, dict)
    assert "id" in record
    assert "person_name" in record
    assert "person_age" in record
    assert "contact_email" in record


def test_extended_nested_structure():
    """Test dynamic handler with extended nested structure (person + contact + address)."""
    extended_df = create_extended_nested_dataframe(3)
    handler = DynamicNestedHandler(extended_df)

    # Test extended properties
    assert len(handler) == 3
    assert len(handler.original_columns) == 4  # id, person, contact, address
    assert len(handler.columns) == 9  # more flattened fields

    # Test address fields exist
    assert "address_street" in handler.columns
    assert "address_zip" in handler.columns
    assert "address_country" in handler.columns

    # Test record access with address
    record = handler[0]
    assert "address_street" in record
    assert "address_country" in record


def test_deeply_nested_structure():
    """Test dynamic handler with deeply nested structure."""
    deep_df = create_deeply_nested_dataframe()
    handler = DynamicNestedHandler(deep_df)

    # Test deep nesting handling (create_deeply_nested_dataframe creates 2 records by default)
    assert len(handler) == 2
    assert len(handler.columns) >= 5  # Multiple levels should be flattened

    # Test deeply nested fields exist
    employee_fields = [col for col in handler.columns if "employee" in col]
    assert len(employee_fields) >= 4  # Should have multiple employee-related fields

    # Test record access with deep nesting
    record = handler[0]
    assert "employee_name" in record

    # Test coordinate fields if they exist
    coord_fields = [col for col in handler.columns if "coordinates" in col]
    if coord_fields:
        lat_fields = [col for col in coord_fields if "lat" in col]
        lon_fields = [col for col in coord_fields if "lon" in col]
        assert len(lat_fields) > 0
        assert len(lon_fields) > 0

        # Test accessing coordinate data
        for lat_field in lat_fields:
            assert lat_field in record
            assert isinstance(record[lat_field], (int, float))


def test_handler_adaptability():
    """Test that handler automatically adapts to any nested structure."""
    # Test with different structures
    structures = [
        create_simple_nested_dataframe(2),
        create_extended_nested_dataframe(2),
        create_deeply_nested_dataframe(),
    ]

    for i, df in enumerate(structures):
        handler = DynamicNestedHandler(df)

        # Each should work regardless of structure
        assert len(handler) >= 2
        assert len(handler.columns) >= 3
        assert len(handler.original_columns) >= 2

        # Should be able to access records
        record = handler[0]
        assert isinstance(record, dict)
        assert len(record) == len(handler.columns)
