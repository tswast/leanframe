#!/usr/bin/env python3
"""
Demo for DynamicNestedHandler comprehensive functionality.

This demonstrates a truly dynamic handler that can work with ANY nested DataFrame structure.
Key features:
- Automatic introspection of nested structures
- Dynamic field extraction and flattening
- Dictionary-like access interface
- Filtering and record access
- Works with arbitrary nesting levels
"""

from tests.unit.nested_data.create_nested_data import create_simple_nested_dataframe, create_extended_nested_dataframe, create_deeply_nested_dataframe
from leanframe.core.nested_handler import DynamicNestedHandler

def demo_basic_usage():
    """Demo basic DynamicNestedHandler usage and structure inspection."""
    print("=== Basic Usage Demo ===")
    df = create_simple_nested_dataframe(5)
    handler = DynamicNestedHandler(df)
    
    print(f"Original columns: {len(handler.original_columns)} - {handler.original_columns}")
    print(f"Extracted columns: {len(handler.columns)} - {handler.columns}")
    print(f"Number of records: {len(handler)}")
    
    # Show available columns
    expected_columns = ['id', 'person_name', 'person_age', 'person_city', 'contact_email', 'contact_phone']
    print(f"Expected columns match: {handler.columns == expected_columns}")
    
    # Show nested fields extraction mapping
    print("\nField extraction mapping:")
    expected_extractions = {
        'person.name': 'person_name',
        'person.age': 'person_age', 
        'person.city': 'person_city',
        'contact.email': 'contact_email',
        'contact.phone': 'contact_phone'
    }
    for original, extracted in expected_extractions.items():
        print(f"  {original} -> {extracted} (✓)" if handler.extracted_fields[original] == extracted else f"  {original} -> {extracted} (✗)")


def demo_data_access():
    """Demo data access patterns of DynamicNestedHandler."""
    print("\n=== Data Access Demo ===")
    df = create_simple_nested_dataframe(3)
    handler = DynamicNestedHandler(df)
    
    # Column-wise access
    names = handler.get_column('person_name')
    ages = handler.get_column('person_age')
    print(f"Names (3 records): {names}")
    print(f"Ages (3 records): {ages}")
    print(f"All names are strings: {all(isinstance(name, str) for name in names)}")
    print(f"All ages are integers: {all(isinstance(age, int) for age in ages)}")
    
    # Record-wise access
    first_record = handler[0]
    print(f"\nFirst record: {first_record}")
    print(f"Record has person_name: {'person_name' in first_record}")
    print(f"Record has contact_email: {'contact_email' in first_record}")
    
    # Dictionary-like interface
    print(f"\nDictionary interface:")
    print(f"'person_name' in handler: {'person_name' in handler}")
    print(f"'nonexistent_column' in handler: {'nonexistent_column' in handler}")
    keys = list(handler.keys())
    print(f"All keys ({len(keys)}): {keys}")


def demo_filtering():
    """Demo filtering functionality of DynamicNestedHandler."""
    print("\n=== Filtering Demo ===")
    df = create_simple_nested_dataframe(5)
    handler = DynamicNestedHandler(df)
    
    print(f"Original handler has {len(handler)} records")
    
    # Filter by age - returns a new handler
    filtered_handler = handler.filter_by('person_age', 30)
    print(f"Filtered handler (age=30) has {len(filtered_handler)} records")
    
    # Show the filtered results
    if len(filtered_handler) > 0:
        print("Filtered records:")
        for i, record in enumerate(filtered_handler):
            print(f"  Record {i}: age={record['person_age']} (✓ = 30)")
    else:
        print("No records found with age=30")


def demo_different_structures():
    """Demo DynamicNestedHandler with different nested structures."""
    print("\n=== Different Structures Demo ===")
    
    # Extended structure with address
    extended_df = create_extended_nested_dataframe(2)
    extended_handler = DynamicNestedHandler(extended_df)
    
    expected_extended_columns = [
        'id', 'person_name', 'person_age', 'person_city',
        'contact_email', 'contact_phone', 
        'address_street', 'address_zip', 'address_country'
    ]
    print(f"Extended structure columns: {extended_handler.columns}")
    print(f"Expected columns match: {extended_handler.columns == expected_extended_columns}")
    
    # Check new address fields
    address_fields = [col for col in extended_handler.columns if 'address' in col]
    print(f"Address fields ({len(address_fields)}): {address_fields}")
    
    # Show a sample record
    if len(extended_handler) > 0:
        sample_record = extended_handler[0]
        print(f"Sample extended record: {sample_record}")


def demo_deep_nesting():
    """Demo DynamicNestedHandler with deeply nested structures."""
    print("\n=== Deep Nesting Demo ===")
    deep_df = create_deeply_nested_dataframe()
    deep_handler = DynamicNestedHandler(deep_df)
    
    print(f"Deep nested structure has {len(deep_handler.columns)} columns")
    print(f"Columns: {deep_handler.columns}")
    
    # Check for deeply nested fields
    deep_fields = [col for col in deep_handler.columns if 'employee' in col]
    print(f"Employee fields ({len(deep_fields)}): {deep_fields}")
    
    # Test access to deeply nested data
    if len(deep_handler) > 0:
        first_record = deep_handler[0]
        print(f"First record has employee_name: {'employee_name' in first_record}")
        
        # Show coordinates if available
        coord_fields = [col for col in deep_handler.columns if 'coordinates' in col]
        if coord_fields:
            print(f"Coordinate fields: {coord_fields}")
            lat_fields = [col for col in coord_fields if 'lat' in col]
            lon_fields = [col for col in coord_fields if 'lon' in col]
            print(f"Has latitude fields: {len(lat_fields) > 0}")
            print(f"Has longitude fields: {len(lon_fields) > 0}")


def demo_handler_capabilities():
    """Demo general capabilities and edge cases of DynamicNestedHandler."""
    print("\n=== Handler Capabilities Demo ===")
    df = create_simple_nested_dataframe(2)
    handler = DynamicNestedHandler(df)
    
    print(f"Handler length: {len(handler)}")
    
    # Iteration demo
    print("\nIteration through records:")
    for i, record in enumerate(handler):
        print(f"  Record {i}: {type(record).__name__} with {len(record)} fields")
        if i == 0:  # Show first record details
            print(f"    Sample fields: {list(record.keys())[:3]}...")
    
    # Column access summary
    print(f"\nColumn summary:")
    print(f"  Flattened columns: {handler.columns}")
    print(f"  Original columns preserved: {handler.original_columns}")
    print(f"  Total fields extracted: {len(handler.columns)}")
    print(f"  Original to extracted ratio: {len(handler.columns) / len(handler.original_columns):.1f}x")


def main():
    """Run all demos."""
    print("🚀 DynamicNestedHandler Comprehensive Demo")
    print("=" * 50)
    
    try:
        demo_basic_usage()
        demo_data_access() 
        demo_filtering()
        demo_different_structures()
        demo_deep_nesting()
        demo_handler_capabilities()
        
        print("\n" + "=" * 50)
        print("✅ All demos completed successfully!")
        print("🎯 DynamicNestedHandler can handle arbitrary nested structures!")
        
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        raise


if __name__ == "__main__":
    main()