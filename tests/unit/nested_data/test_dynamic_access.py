#!/usr/bin/env python3
"""Test the dictionary-like access interface of the DynamicNestedHandler"""

from tests.unit.nested_data.create_nested_data import create_simple_nested_dataframe, create_extended_nested_dataframe
from leanframe.core.nested_handler import DynamicNestedHandler

def test_dictionary_access():
    """Test dictionary-like access interface of DynamicNestedHandler."""
    # Create a simple nested DataFrame
    df = create_simple_nested_dataframe()
    handler = DynamicNestedHandler(df)
    
    # Test keys are available
    keys = list(handler.keys())
    expected_keys = ['id', 'person_name', 'person_age', 'person_city', 'contact_email', 'contact_phone']
    assert keys == expected_keys
    
    # Test individual field access
    person_names = handler.get_column('person_name')
    person_ages = handler.get_column('person_age')
    contact_emails = handler.get_column('contact_email')
    
    assert len(person_names) == 3  # Default is 3 records
    assert len(person_ages) == 3
    assert len(contact_emails) == 3
    
    # Test iteration
    items_count = 0
    for key, values in handler.items():
        assert key in expected_keys
        assert len(values) == 3
        items_count += 1
    assert items_count == len(expected_keys)
    
    # Test membership
    assert 'person_name' in handler
    assert 'contact_email' in handler
    assert 'nonexistent_field' not in handler


def test_extended_structure_access():
    """Test dictionary access with extended nested structure."""
    extended_df = create_extended_nested_dataframe()
    extended_handler = DynamicNestedHandler(extended_df)
    
    # Test extended keys include address fields
    keys = list(extended_handler.keys())
    assert 'address_street' in keys
    assert 'address_country' in keys
    assert 'address_zip' in keys
    
    # Test access to address fields
    streets = extended_handler.get_column('address_street')
    countries = extended_handler.get_column('address_country')
    
    assert len(streets) > 0
    assert len(countries) > 0
    assert all(isinstance(street, str) for street in streets)
    assert all(isinstance(country, str) for country in countries)