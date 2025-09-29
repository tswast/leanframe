#!/usr/bin/env python3
"""
QUICK REFERENCE: PyArrow nested tables → leanframe DataFrames

Essential code pattern for converting PyArrow nested tables to leanframe DataFrames.
"""

import pyarrow as pa
import pandas as pd
from tests.unit.nested_data.create_nested_data import pyarrow_to_leanframe


def convert_pyarrow_to_leanframe(pa_table):
    """
    Convert PyArrow table with nested data to leanframe DataFrame.
    
    Args:
        pa_table: PyArrow table with nested structures
        
    Returns:
        leanframe DataFrame
    """
    return pyarrow_to_leanframe(pa_table)


def safe_to_pandas(lf_df):
    """
    Convert leanframe DataFrame back to pandas (when possible).
    Works well for simple operations, may have edge cases for complex ones.
    """
    try:
        pyarrow_result = lf_df._data.to_pyarrow()
        table = pyarrow_result.read_all()
        return table.to_pandas(types_mapper=pd.ArrowDtype)
    except Exception as e:
        print(f"Conversion error: {e}")
        return None


# Example usage:
if __name__ == "__main__":
    from tests.unit.nested_data.create_nested_data import get_person_schema, create_basic_person_contact_data
    
    # Create nested PyArrow table using centralized utilities
    data = create_basic_person_contact_data(3)
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("person", get_person_schema())
    ])
    
    # Only include person data for this simple example
    simple_data = {
        "id": data["id"],
        "person": data["person"]
    }
    
    pa_table = pa.Table.from_pydict(simple_data, schema=schema)
    
    # Convert to leanframe DataFrame
    lf_df = convert_pyarrow_to_leanframe(pa_table)
    
    # Use leanframe operations
    lf_df_enhanced = lf_df.assign(status="active")
    
    # Convert back to pandas (when needed)
    result = safe_to_pandas(lf_df_enhanced)
    
    if result is not None:
        print("Success! Result:")
        print(result)
        print(f"Struct access: {result['person'].struct.field('name').tolist()}")
    else:
        print("Conversion back to pandas failed, but leanframe operations work!")