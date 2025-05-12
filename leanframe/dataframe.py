# Copyright 2023 Google LLC, LeanFrame Authors
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

"""DataFrame is a two dimensional data structure."""

from __future__ import annotations

import re
import typing
from typing import (
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import google.cloud.bigquery as bigquery
import ibis
import ibis.expr.datatypes as ibis_dtypes
import ibis.expr.types as ibis_types
import pandas as pd

import leanframe.aggregations as agg_ops
import leanframe.core
import leanframe.core.blocks as blocks
import leanframe.core.groupby as groupby
import leanframe.core.indexes as indexes
import leanframe.core.joins as joins
import leanframe.core.ordering as order
import leanframe.operations.dtypes
import leanframe.operations as ops
import leanframe.series
import third_party.leanframe_vendored.pandas.pandas.io.common as vendored_pandas_io_common


class DataFrame:
    """A 2D data structure, representing data and deferred computation."""

    def __init__( self):
        pass

    def to_pandas(self):
        pass

    def to_gbq(self):
        pass
