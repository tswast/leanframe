# Copyright 2025 Google LLC, LeanFrame Authors
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

import pathlib

import ibis
import pytest

import leanframe


CURRENT_DIR = pathlib.Path(__file__).parent
DATA_DIR = CURRENT_DIR.parent / "data"


@pytest.fixture(scope="session")
def session() -> leanframe.Session:
    """A Session based on a local engine for unit testing."""
    backend = ibis.duckdb.connect()

    # Create a few test tables before 
    backend.raw_sql(
        f"""
        CREATE TABLE veggies AS
        SELECT * FROM read_csv('{str(DATA_DIR / "veggies.csv")}');
        """
    )

    return leanframe.Session(backend)
