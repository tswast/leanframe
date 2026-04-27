# leanframe

(This is not an official Google product.)

LeanFrame is a pandas-like API built on top of Ibis. This is a fork of an early
version of bigframes (https://github.com/googleapis/python-bigquery-dataframes)
and is intended to be a minimal wrapper on top of Ibis without any of the pandas
features that make bigframes more complex.

## Installation

LeanFrame is available on [PyPI](https://pypi.org/project/leanframe/).

```
pip install leanframe
```

Also, install the dependencies for an Ibis backend, such as

```
pip install 'ibis-framework[bigquery]'
```

## Usage

To start using LeanFrame, first create a Session from an Ibis backend, such as
[DuckDB (local)](https://ibis-project.org/backends/duckdb) or [BigQuery (Google
Cloud)](https://ibis-project.org/backends/bigquery).

```python
import ibis
import leanframe

connection = ibis.bigquery.connect(
    project_id="your-project-id",
)
session = leanframe.Session(connection)
```

Create a DataFrame from a table.

```python
df = session.read_sql_table("bigquery-public-data.usa_names.usa_1910_2013")
```

Perform pandas operations.

```
TODO
```

Get your results as a pandas DataFrame.

```python
pddf = df.to_pandas()
```

## Disclaimer

This is a personal project. Does not reflect my employer.
