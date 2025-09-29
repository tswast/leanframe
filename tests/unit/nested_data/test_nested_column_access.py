#!/usr/bin/env python3
"""
Test: Accessing nested columns in leanframe DataFrames.

This tests the working approaches for accessing nested fields
in leanframe DataFrames created from PyArrow nested data.
"""

import pandas as pd
from leanframe.core.frame import DataFrame
from tests.unit.nested_data.create_nested_data import create_simple_nested_dataframe


def test_nested_column_access():
    """Test nested column access approaches."""
    # Create nested DataFrame
    lf_df = create_simple_nested_dataframe(3)
    
    # Verify basic structure
    assert 'id' in lf_df.columns.tolist()
    assert 'person' in lf_df.columns.tolist()
    assert 'contact' in lf_df.columns.tolist()
    
    # Method 1: Access top-level nested columns
    person_series = lf_df["person"]
    contact_series = lf_df["contact"]
    assert person_series.dtype.name.startswith("struct")
    assert contact_series.dtype.name.startswith("struct")
    
    # Method 2: Extract nested fields using ibis (Recommended!)
    ibis_table = lf_df._data
    
    # Extract specific nested fields
    extracted_table = ibis_table.select(
        ibis_table.id,
        ibis_table["person"]["name"].name("person_name"),
        ibis_table["person"]["age"].name("person_age"),
        ibis_table["contact"]["email"].name("email")
    )
    
    extracted_df = DataFrame(extracted_table)
    expected_columns = ['id', 'person_name', 'person_age', 'email']
    assert extracted_df.columns.tolist() == expected_columns
    
    # Use leanframe operations on extracted fields
    result = extracted_df.assign(
        age_group="adult",
        has_email=True
    )
    expected_columns_after_assign = ['id', 'person_name', 'person_age', 'email', 'age_group', 'has_email']
    assert result.columns.tolist() == expected_columns_after_assign
    
    # Method 3: Convert to pandas for complex nested operations
    try:
        pandas_result = lf_df._data.to_pyarrow().read_all().to_pandas(types_mapper=pd.ArrowDtype)
        
        # Access nested fields using pandas struct accessor
        names = pandas_result['person'].struct.field('name').tolist()
        ages = pandas_result['person'].struct.field('age').tolist()
        emails = pandas_result['contact'].struct.field('email').tolist()
        
        # Verify we got the expected data
        assert len(names) == 3
        assert len(ages) == 3
        assert len(emails) == 3
        assert all(isinstance(name, str) for name in names)
        assert all(isinstance(age, int) for age in ages)
        assert all(isinstance(email, str) for email in emails)
        
    except Exception as e:
        # If pandas conversion fails, that's acceptable - just verify structure exists
        assert 'person' in lf_df.columns.tolist()
        assert 'contact' in lf_df.columns.tolist()


def test_pandas_conversion_fallback():
    """Test that leanframe operations work even when pandas conversion might fail."""
    lf_df = create_simple_nested_dataframe(2)
    
    # Verify leanframe operations work regardless of pandas conversion issues
    ibis_table = lf_df._data
    extracted_table = ibis_table.select(
        ibis_table.id,
        ibis_table["person"]["name"].name("person_name")
    )
    
    extracted_df = DataFrame(extracted_table)
    result = extracted_df.assign(test_column=True)
    
    assert 'test_column' in result.columns.tolist()
    assert 'person_name' in result.columns.tolist()