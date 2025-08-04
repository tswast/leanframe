# Gemini Development Guide for leanframe

This guide provides context for developing the `leanframe` project using LLM-based tools.

## Project Overview

`leanframe` is a pandas-like API built on top of the Ibis project. It is intended to be a minimal wrapper around Ibis, providing a familiar interface for data manipulation without the more complex features of pandas.

## Getting Started

To set up the development environment, you will need `uv`, which is a fast Python package installer and resolver.

1.  Install `uv`:
    ```bash
    pip install uv
    ```

2.  Create a virtual environment and install dependencies:
    ```bash
    uv sync
    ```

## Development Workflow

This project uses `pytest` for testing, `ruff` for linting and formatting, and `mypy` for static type checking.

-   **Running tests:**
    ```bash
    uv run pytest
    ```

-   **Linting:**
    ```bash
    uv run ruff check
    ```

-   **Formatting:**
    ```bash
    uv run ruff format
    ```

-   **Type checking:**
    ```bash
    uv run mypy leanframe tests
    ```

## Key Technologies

-   **`pandas`**: Used for the API and data structures.
-   **`pyarrow`**: For efficient in-memory data representation.
-   **`ibis-framework`**: The core backend for deferred execution.
-   **`duckdb`**: The default backend for running tests.
-   **`pytest`**: The testing framework.
-   **`ruff`**: The linter and formatter.
-   **`mypy`**: The static type checker.
