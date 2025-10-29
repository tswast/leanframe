# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Helpers to convert between ibis and pandas dtypes."""

from __future__ import annotations

import ibis
import ibis.expr.datatypes as ibis_dtypes
import pandas as pd


def convert_ibis_to_pandas(
    ibis_type: ibis_dtypes.DataType,
) -> pd.ArrowDtype:
    """
    Convert an ibis type to a pandas ArrowDtype.

    Args:
        ibis_type: The ibis type to convert.

    Returns:
        The corresponding pandas ArrowDtype.
    """
    arrow_type = ibis_type.to_pyarrow()
    return pd.ArrowDtype(arrow_type)


def convert_pandas_to_ibis(
    pandas_type: pd.ArrowDtype,
) -> ibis_dtypes.DataType:
    """
    Convert a pandas ArrowDtype to an ibis type.

    Args:
        pandas_type: The pandas type to convert.

    Returns:
        The corresponding ibis type.
    """
    arrow_type = pandas_type.pyarrow_dtype
    return ibis.dtype(arrow_type)
