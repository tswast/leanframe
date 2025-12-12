
import pytest
import pandas as pd
import leanframe as lf
import ibis
import ibis.expr.types as ibis_types
from leanframe.core.frame import DataFrame
from leanframe.core.series import Series

def test_dataframe_to_ibis():
    # Setup
    con = ibis.sqlite.connect()
    t = con.create_table('test_df_ibis', schema=ibis.schema({'a': 'int64', 'b': 'string'}))
    df = DataFrame(t)

    # Execute
    expr = df.to_ibis()

    # Assert
    assert isinstance(expr, ibis_types.Table)
    assert expr.equals(t)

def test_series_to_ibis():
    # Setup
    con = ibis.sqlite.connect()
    t = con.create_table('test_series_ibis', schema=ibis.schema({'a': 'int64'}))
    s = Series(t['a'])

    # Execute
    expr = s.to_ibis()

    # Assert
    assert isinstance(expr, ibis_types.Column)
    assert expr.equals(t['a'])
