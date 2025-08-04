# Configure GitHub Actions

Complete the GitHub Actions configuration with linter and typing checks.

## Background

To ensure code quality, it is important to automatically run our checkers, even
if the developer doesn't remember to.

## Acceptance Criteria

- [ ] A GitHub action exists and runs correctly for the linter.
- [ ] A GitHub action exists and runs correctly for the type checker.

## Detailed Steps

- [ ] Configure `.github/workflows/lint.yml` to run on push and pull requests.
- [ ] Add a job to `lint.yml` that checks out the code, sets up Python, and installs dependencies with `uv`.
- [ ] Add a step to the `lint.yml` job to run the linter with `uv run ruff check`.
- [ ] Configure `.github/workflows/mypy.yml` to run on push and pull requests.
- [ ] Add a job to `mypy.yml` that checks out the code, sets up Python, and installs dependencies with `uv`.
- [ ] Add a step to the `mypy.yml` job to run the type checker with `uv run mypy leanframe tests`.

## Verification

*Specify the commands to run to verify the changes.*

- [ ] All new and existing tests `uv run pytest tests` should pass.
- [ ] The `uv run mypy leanframe tests` static type checker should pass.
- [ ] The `uv run ruff check` linter should pass.
- [ ] Only add git commits. Do not change git history.

## Constraints

Follow the guidelines listed in GEMINI.md at the root of the repository.
