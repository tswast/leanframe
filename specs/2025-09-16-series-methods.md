# Series methods

Implement scalar, aggregate, and window methods for leanframe Series objects.

## Background

The leanframe package aims to provide a pandas-compatible interface while
maintaining a 1:1 mapping to related Ibis types. DataFrame is 1:1 with Ibis
Table, and Series is 1:1 with Ibis Column.

Pandas features not supported by Ibis, such as specifying index column(s) or a
total ordering over all rows are also not supported by leanframe.

## Acceptance Criteria

Implement each pandas Series method that doesn't require an index or
ordering. When an item is completed, edit `specs/2025-09-16-series-methods.md`
and check off the item with an x.

- [x] pandas.Series.index -- not feasible, requires index
- [ ] pandas.Series.array
- [x] pandas.Series.values
- [x] pandas.Series.dtype
- [x] pandas.Series.shape
- [x] pandas.Series.nbytes
- [x] pandas.Series.ndim
- [x] pandas.Series.size
- [x] pandas.Series.T -- not feasible
- [ ] pandas.Series.memory_usage
- [x] pandas.Series.hasnans
- [ ] pandas.Series.empty
- [ ] pandas.Series.dtypes
- [x] pandas.Series.name
- [ ] pandas.Series.flags
- [ ] pandas.Series.set_flags
- [ ] pandas.Series.astype
- [ ] pandas.Series.convert_dtypes
- [ ] pandas.Series.infer_objects
- [ ] pandas.Series.copy
- [ ] pandas.Series.bool
- [ ] pandas.Series.to_numpy
- [ ] pandas.Series.to_period
- [ ] pandas.Series.to_timestamp
- [ ] pandas.Series.to_list
- [ ] pandas.Series.__array__
- [ ] pandas.Series.get
- [x] pandas.Series.at -- not feasible, requires index
- [x] pandas.Series.iat -- not feasible, requires ordering
- [x] pandas.Series.loc -- not feasible, requires index
- [x] pandas.Series.iloc -- not feasible, requires ordering
- [ ] pandas.Series.__iter__
- [x] pandas.Series.items -- not feasible, requires index
- [ ] pandas.Series.keys -- not feasible, requires index
- [ ] pandas.Series.pop -- not feasible, requires index
- [ ] pandas.Series.item
- [ ] pandas.Series.xs
- [ ] pandas.Series.add
- [ ] pandas.Series.sub
- [ ] pandas.Series.mul
- [ ] pandas.Series.div
- [ ] pandas.Series.truediv
- [ ] pandas.Series.floordiv
- [ ] pandas.Series.mod
- [ ] pandas.Series.pow
- [ ] pandas.Series.radd
- [ ] pandas.Series.rsub
- [ ] pandas.Series.rmul
- [ ] pandas.Series.rdiv
- [ ] pandas.Series.rtruediv
- [ ] pandas.Series.rfloordiv
- [ ] pandas.Series.rmod
- [ ] pandas.Series.rpow
- [ ] pandas.Series.combine
- [ ] pandas.Series.combine_first
- [x] pandas.Series.round
- [x] pandas.Series.lt
- [x] pandas.Series.gt
- [x] pandas.Series.le
- [x] pandas.Series.ge
- [x] pandas.Series.ne
- [x] pandas.Series.eq
- [ ] pandas.Series.product
- [ ] pandas.Series.dot
- [ ] pandas.Series.apply
- [ ] pandas.Series.agg
- [ ] pandas.Series.aggregate
- [ ] pandas.Series.transform
- [ ] pandas.Series.map
- [ ] pandas.Series.groupby
- [ ] pandas.Series.rolling
- [ ] pandas.Series.expanding
- [ ] pandas.Series.ewm
- [ ] pandas.Series.pipe
- [ ] pandas.Series.abs
- [ ] pandas.Series.all
- [ ] pandas.Series.any
- [ ] pandas.Series.autocorr
- [ ] pandas.Series.between
- [ ] pandas.Series.clip
- [ ] pandas.Series.corr
- [ ] pandas.Series.count
- [ ] pandas.Series.cov
- [ ] pandas.Series.cummax
- [ ] pandas.Series.cummin
- [ ] pandas.Series.cumprod
- [ ] pandas.Series.cumsum
- [ ] pandas.Series.describe
- [ ] pandas.Series.diff
- [ ] pandas.Series.factorize
- [ ] pandas.Series.kurt
- [x] pandas.Series.max
- [x] pandas.Series.mean
- [ ] pandas.Series.median
- [x] pandas.Series.min
- [ ] pandas.Series.mode
- [ ] pandas.Series.nlargest
- [ ] pandas.Series.nsmallest
- [ ] pandas.Series.pct_change
- [ ] pandas.Series.prod
- [ ] pandas.Series.quantile
- [ ] pandas.Series.rank
- [ ] pandas.Series.sem
- [ ] pandas.Series.skew
- [x] pandas.Series.std
- [x] pandas.Series.sum
- [x] pandas.Series.var
- [ ] pandas.Series.kurtosis
- [ ] pandas.Series.unique
- [ ] pandas.Series.nunique
- [ ] pandas.Series.is_unique
- [ ] pandas.Series.is_monotonic_increasing
- [ ] pandas.Series.is_monotonic_decreasing
- [ ] pandas.Series.value_counts
- [ ] pandas.Series.align
- [ ] pandas.Series.case_when
- [ ] pandas.Series.drop
- [ ] pandas.Series.droplevel
- [ ] pandas.Series.drop_duplicates
- [ ] pandas.Series.duplicated
- [ ] pandas.Series.equals
- [ ] pandas.Series.first
- [ ] pandas.Series.head
- [ ] pandas.Series.idxmax
- [ ] pandas.Series.idxmin
- [ ] pandas.Series.isin
- [ ] pandas.Series.last
- [ ] pandas.Series.reindex
- [ ] pandas.Series.reindex_like
- [ ] pandas.Series.rename
- [ ] pandas.Series.rename_axis
- [ ] pandas.Series.reset_index
- [ ] pandas.Series.sample
- [ ] pandas.Series.set_axis
- [ ] pandas.Series.take
- [ ] pandas.Series.tail
- [ ] pandas.Series.truncate
- [ ] pandas.Series.where
- [ ] pandas.Series.mask
- [ ] pandas.Series.add_prefix
- [ ] pandas.Series.add_suffix
- [ ] pandas.Series.filter
- [ ] pandas.Series.backfill
- [ ] pandas.Series.bfill
- [ ] pandas.Series.dropna
- [ ] pandas.Series.ffill
- [ ] pandas.Series.fillna
- [ ] pandas.Series.interpolate
- [ ] pandas.Series.isna
- [ ] pandas.Series.isnull
- [ ] pandas.Series.notna
- [ ] pandas.Series.notnull
- [ ] pandas.Series.pad
- [ ] pandas.Series.replace
- [ ] pandas.Series.argsort
- [ ] pandas.Series.argmin
- [ ] pandas.Series.argmax
- [ ] pandas.Series.reorder_levels
- [ ] pandas.Series.sort_values
- [ ] pandas.Series.sort_index
- [ ] pandas.Series.swaplevel
- [ ] pandas.Series.unstack
- [ ] pandas.Series.explode
- [ ] pandas.Series.searchsorted
- [ ] pandas.Series.ravel
- [ ] pandas.Series.repeat
- [ ] pandas.Series.squeeze
- [ ] pandas.Series.view
- [ ] pandas.Series.compare
- [ ] pandas.Series.update
- [ ] pandas.Series.asfreq
- [ ] pandas.Series.asof
- [ ] pandas.Series.shift
- [ ] pandas.Series.first_valid_index
- [ ] pandas.Series.last_valid_index
- [ ] pandas.Series.resample
- [ ] pandas.Series.tz_convert
- [ ] pandas.Series.tz_localize
- [ ] pandas.Series.at_time
- [ ] pandas.Series.between_time
- [ ] pandas.Series.str
- [ ] pandas.Series.cat
- [ ] pandas.Series.dt
- [ ] pandas.Series.sparse
- [ ] pandas.Series.attrs
- [ ] pandas.Series.hist
- [ ] pandas.Series.to_pickle
- [ ] pandas.Series.to_csv
- [ ] pandas.Series.to_dict
- [ ] pandas.Series.to_excel
- [ ] pandas.Series.to_frame
- [ ] pandas.Series.to_xarray
- [ ] pandas.Series.to_hdf
- [ ] pandas.Series.to_sql
- [ ] pandas.Series.to_json
- [ ] pandas.Series.to_string
- [ ] pandas.Series.to_clipboard
- [ ] pandas.Series.to_latex
- [ ] pandas.Series.to_markdown

## Detailed Steps

This document outlines the steps to implement new Series methods. Follow these
steps carefully.

### 1. Understand the Task

- [ ] **Read this document carefully**: Before you begin, read this entire
      document (`specs/2025-09-16-series-methods.md`) to understand the scope
      and requirements of the task.

### 2. Implement a Method

- [ ] **Choose a method**: Select an unchecked method from the `Acceptance
      Criteria` list above.
- [ ] **Feasibility check**: Determine if the method is feasible to implement
      given the constraints mentioned in the `Background` section.
- [ ] **Mark as complete or infeasible**:
    - If the method is **feasible**, continue to the next step.
    - If the method is **not feasible**, mark it with an `x` in the `Acceptance
      Criteria` list and add a brief note explaining why (e.g., `- [x]
      pandas.Series.index -- not feasible, requires index`).
- [ ] **Implement the method**: Add the method to `leanframe/core/series.py`.
      Ensure your implementation is consistent with the existing codebase.
- [ ] **Add unit tests**: Create comprehensive unit tests for the new method in
      `tests/unit/test_series.py`. Cover edge cases and different data types.

### 3. Verify Your Changes

- [ ] **Run tests**: Execute all tests by running `uv run pytest tests` to
      ensure your changes haven't introduced any regressions.
- [ ] **Run static analysis**: Run `uv run mypy leanframe tests` and `uv run
      ruff check` to check for type errors and linting issues.
- [ ] **Mark the method as complete**: Once the implementation is complete and
      all checks pass, edit this file (`specs/2025-09-16-series-methods.md`) and
      mark the method you implemented with an `x` in the `Acceptance Criteria`
      list.

### 4. Finalizing for Submission

- [ ] **Reset checkboxes**: Before submitting your pull request, uncheck all the
      boxes in the `Acceptance Criteria` that you have marked with an `x` during
      your work. The spec file should be in a clean state for the next
      developer. Leave the originally checked items as they are.

## Verification

*Specify the commands to run to verify the changes.*

- [ ] All new and existing tests `uv run pytest tests` should pass.
- [ ] The `uv run mypy leanframe tests` static type checker should pass.
- [ ] The `uv run ruff check` linter should pass.
- [ ] Only add git commits. Do not change git history.

## Constraints

Follow the guidelines listed in GEMINI.md at the root of the repository.
