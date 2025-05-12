# Copyright 2023 Google LLC
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

import logging
import pathlib
import hashlib
from typing import cast, Dict, Generator, Optional

import pytest
from google.cloud import bigquery

import leanframe.dataframe
import leanframe.session


PERMANENT_DATASET = "leanframe_testing"

CURRENT_DIR = pathlib.Path(__file__).parent
DATA_DIR = CURRENT_DIR.parent / "data"



def _hash_digest_file(hasher, filepath):
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)




def load_test_data(
    table_id: str,
    bigquery_client: bigquery.Client,
    schema_filename: str,
    data_filename: str,
    location: Optional[str],
) -> bigquery.LoadJob:
    """Create a temporary table with test data"""
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = tuple(
        bigquery_client.schema_from_json(DATA_DIR / schema_filename)
    )
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    with open(DATA_DIR / data_filename, "rb") as input_file:
        # TODO(swast): Location is allowed to be None in BigQuery Client.
        # Can remove after
        # https://github.com/googleapis/python-bigquery/pull/1554 is released.
        location = "US" if location is None else location
        job = bigquery_client.load_table_from_file(
            input_file,
            table_id,
            job_config=job_config,
            location=location,
        )
    # No cleanup necessary, as the surrounding dataset will delete contents.
    return cast(bigquery.LoadJob, job.result())


def load_test_data_tables(
    session: leanframe.session.Session, dataset_id_permanent: str
) -> Dict[str, str]:
    """Returns cached references to the test data tables in BigQuery. If no matching table is found
    for the hash of the data and schema, the table will be uploaded."""
    existing_table_ids = [
        table.table_id for table in session.bqclient.list_tables(dataset_id_permanent)
    ]
    table_mapping: Dict[str, str] = {}
    for table_name, schema_filename, data_filename in [
        ("scalars", "scalars_schema.json", "scalars.jsonl"),
        ("scalars_too", "scalars_schema.json", "scalars.jsonl"),
        ("nested", "nested_schema.json", "nested.jsonl"),
        ("nested_structs", "nested_structs_schema.json", "nested_structs.jsonl"),
        ("repeated", "repeated_schema.json", "repeated.jsonl"),
        ("json", "json_schema.json", "json.jsonl"),
        ("penguins", "penguins_schema.json", "penguins.jsonl"),
        ("ratings", "ratings_schema.json", "ratings.jsonl"),
        ("time_series", "time_series_schema.json", "time_series.jsonl"),
        ("hockey_players", "hockey_players.json", "hockey_players.jsonl"),
        ("matrix_2by3", "matrix_2by3.json", "matrix_2by3.jsonl"),
        ("matrix_3by4", "matrix_3by4.json", "matrix_3by4.jsonl"),
        ("urban_areas", "urban_areas_schema.json", "urban_areas.jsonl"),
    ]:
        test_data_hash = hashlib.md5()
        _hash_digest_file(test_data_hash, DATA_DIR / schema_filename)
        _hash_digest_file(test_data_hash, DATA_DIR / data_filename)
        test_data_hash.update(table_name.encode())
        target_table_id = f"{table_name}_{test_data_hash.hexdigest()}"
        target_table_id_full = f"{dataset_id_permanent}.{target_table_id}"
        if target_table_id not in existing_table_ids:
            # matching table wasn't found in the permanent dataset - we need to upload it
            logging.info(
                f"Test data table {table_name} was not found in the permanent dataset, regenerating it..."
            )
            load_test_data(
                target_table_id_full,
                session.bqclient,
                schema_filename,
                data_filename,
                location=session._location,
            )

        table_mapping[table_name] = target_table_id_full

    return table_mapping


@pytest.fixture(scope="session")
def session() -> Generator[leanframe.session.Session, None, None]:
    context = leanframe.session.Context(location="US")
    session = leanframe.session.Session(context=context)
    yield session
    session.close()  # close generated session at cleanup time


@pytest.fixture(scope="session")
def bigquery_client(session: leanframe.session.Session) -> bigquery.Client:
    return session.bqclient


@pytest.fixture(scope="session")
def dataset_id_permanent(bigquery_client: bigquery.Client, project_id: str) -> str:
    """Create a dataset if it doesn't exist."""
    dataset_id = f"{project_id}.{PERMANENT_DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    bigquery_client.create_dataset(dataset, exists_ok=True)
    return dataset_id


@pytest.fixture(scope="session")
def test_data_tables(
    session: leanframe.session.Session, dataset_id_permanent: str
) -> Dict[str, str]:
    return load_test_data_tables(session, dataset_id_permanent)


@pytest.fixture(scope="session")
def scalars_table_id(test_data_tables) -> str:
    return test_data_tables["scalars"]


@pytest.fixture(scope="session")
def scalars_df(
    scalars_table_id: str, session: leanframe.session.Session
) -> leanframe.dataframe.DataFrame:
    """DataFrame pointing at test data."""
    return session.read_gbq(scalars_table_id)
