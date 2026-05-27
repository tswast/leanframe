
import ibis
import pandas as pd
import pytest
from unittest.mock import MagicMock

from leanframe.core import session

@pytest.fixture
def mock_backend():
    return MagicMock()

def test_read_ibis_returns_dataframe(mock_backend):
    """Test that read_ibis returns a leanframe DataFrame."""
    s = session.Session(mock_backend)
    df = pd.DataFrame({'a': [1, 2, 3]})
    t = ibis.memtable(df)

    lf_df = s.read_ibis(t)

    # Check that it is a leanframe DataFrame
    # Note: importing DataFrame inside the function to avoid circular imports if any,
    # though it should be fine at module level for type checking
    from leanframe.core.frame import DataFrame
    assert isinstance(lf_df, DataFrame)

    # Check that the underlying data is correct
    assert lf_df.to_ibis().equals(t)

def test_read_ibis_with_complex_expression(mock_backend):
    """Test read_ibis with a more complex ibis expression."""
    s = session.Session(mock_backend)
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    t = ibis.memtable(df)
    expr = t.mutate(c=t.a + t.b)

    lf_df = s.read_ibis(expr)

    from leanframe.core.frame import DataFrame
    assert isinstance(lf_df, DataFrame)
    assert lf_df.to_ibis().equals(expr)
