# DataFrame methods

Implement scalar, aggregate, and window methods for leanframe DataFrame objects.

## Background

The leanframe package aims to provide a pandas-compatible interface while maintaining a 1:1 mapping to related Ibis types. DataFrame is 1:1 with Ibis Table, and Series is 1:1 with Ibis Column.

Pandas features not supported by Ibis, such as specifying index column(s) or a total ordering over all rows are also not supported by leanframe.

## Acceptance Criteria

Implement each pandas DataFrame method that doesn't require an index or ordering. When an item is completed, edit specs/2025-10-27-dataframe-methods.md and check off the item with an x.

### Attributes

- [ ] T
- [ ] at
- [ ] attrs
- [ ] axes
- [ ] columns
- [ ] dtypes
- [ ] empty
- [ ] flags
- [ ] iat
- [ ] iloc
- [ ] index
- [ ] loc
- [ ] ndim
- [ ] shape
- [ ] size
- [ ] style
- [ ] values

### Methods

- [ ] abs
- [ ] add(other[, axis, level, fill_value])
- [ ] add_prefix(prefix[, axis])
- [ ] add_suffix(suffix[, axis])
- [ ] agg([func, axis])
- [ ] aggregate([func, axis])
- [ ] align(other[, join, axis, level, copy, ...])
- [ ] all([axis, bool_only, skipna])
- [ ] any(*[, axis, bool_only, skipna])
- [ ] apply(func[, axis, raw, result_type, args, ...])
- [ ] applymap(func[, na_action])
- [ ] asfreq(freq[, method, how, normalize, ...])
- [ ] asof(where[, subset])
- [ ] assign(**kwargs)
- [ ] astype(dtype[, copy, errors])
- [ ] at_time(time[, asof, axis])
- [ ] backfill(*[, axis, inplace, limit, downcast])
- [ ] between_time(start_time, end_time[, ...])
- [ ] bfill(*[, axis, inplace, limit, limit_area, ...])
- [ ] bool()
- [ ] boxplot([column, by, ax, fontsize, rot, ...])
- [ ] clip([lower, upper, axis, inplace])
- [ ] combine(other, func[, fill_value, overwrite])
- [ ] combine_first(other)
- [ ] compare(other[, align_axis, keep_shape, ...])
- [ ] convert_dtypes([infer_objects, ...])
- [ ] copy([deep])
- [ ] corr([method, min_periods, numeric_only])
- [ ] corrwith(other[, axis, drop, method, ...])
- [ ] count([axis, numeric_only])
- [ ] cov([min_periods, ddof, numeric_only])
- [ ] cummax([axis, skipna])
- [ ] cummin([axis, skipna])
- [ ] cumprod([axis, skipna])
- [ ] cumsum([axis, skipna])
- [ ] describe([percentiles, include, exclude])
- [ ] diff([periods, axis])
- [ ] div(other[, axis, level, fill_value])
- [ ] divide(other[, axis, level, fill_value])
- [ ] dot(other)
- [ ] drop([labels, axis, index, columns, level, ...])
- [ ] drop_duplicates([subset, keep, inplace, ...])
- [ ] droplevel(level[, axis])
- [ ] dropna(*[, axis, how, thresh, subset, ...])
- [ ] duplicated([subset, keep])
- [ ] eq(other[, axis, level])
- [ ] equals(other)
- [ ] eval(expr, *[, inplace])
- [ ] ewm([com, span, halflife, alpha, ...])
- [ ] expanding([min_periods, axis, method])
- [ ] explode(column[, ignore_index])
- [ ] ffill(*[, axis, inplace, limit, limit_area, ...])
- [ ] fillna([value, method, axis, inplace, ...])
- [ ] filter([items, like, regex, axis])
- [ ] first(offset)
- [ ] first_valid_index()
- [ ] floordiv(other[, axis, level, fill_value])
- [ ] from_dict(data[, orient, dtype, columns])
- [ ] from_records(data[, index, exclude, ...])
- [ ] ge(other[, axis, level])
- [ ] get(key[, default])
- [ ] groupby([by, axis, level, as_index, sort, ...])
- [ ] gt(other[, axis, level])
- [ ] head([n])
- [ ] hist([column, by, grid, xlabelsize, xrot, ...])
- [ ] idxmax([axis, skipna, numeric_only])
- [ ] idxmin([axis, skipna, numeric_only])
- [ ] infer_objects([copy])
- [ ] info([verbose, buf, max_cols, memory_usage, ...])
- [ ] insert(loc, column, value[, allow_duplicates])
- [ ] interpolate([method, axis, limit, inplace, ...])
- [ ] isetitem(loc, value)
- [ ] isin(values)
- [ ] isna()
- [ ] isnull()
- [ ] items()
- [ ] iterrows()
- [ ] itertuples([index, name])
- [ ] join(other[, on, how, lsuffix, rsuffix, ...])
- [ ] keys()
- [ ] kurt([axis, skipna, numeric_only])
- [ ] kurtosis([axis, skipna, numeric_only])
- [ ] last(offset)
- [ ] last_valid_index()
- [ ] le(other[, axis, level])
- [ ] lt(other[, axis, level])
- [ ] map(func[, na_action])
- [ ] mask(cond[, other, inplace, axis, level])
- [ ] max([axis, skipna, numeric_only])
- [ ] mean([axis, skipna, numeric_only])
- [ ] median([axis, skipna, numeric_only])
- [ ] melt([id_vars, value_vars, var_name, ...])
- [ ] memory_usage([index, deep])
- [ ] merge(right[, how, on, left_on, right_on, ...])
- [ ] min([axis, skipna, numeric_only])
- [ ] mod(other[, axis, level, fill_value])
- [ ] mode([axis, numeric_only, dropna])
- [ ] mul(other[, axis, level, fill_value])
- [ ] multiply(other[, axis, level, fill_value])
- [ ] ne(other[, axis, level])
- [ ] nlargest(n, columns[, keep])
- [ ] notna()
- [ ] notnull()
- [ ] nsmallest(n, columns[, keep])
- [ ] nunique([axis, dropna])
- [ ] pad(*[, axis, inplace, limit, downcast])
- [ ] pct_change([periods, fill_method, limit, freq])
- [ ] pipe(func, *args, **kwargs)
- [ ] pivot(*, columns[, index, values])
- [ ] pivot_table([values, index, columns, ...])
- [ ] pop(item)
- [ ] pow(other[, axis, level, fill_value])
- [ ] prod([axis, skipna, numeric_only, min_count])
- [ ] product([axis, skipna, numeric_only, min_count])
- [ ] quantile([q, axis, numeric_only, ...])
- [ ] query(expr, *[, inplace])
- [ ] radd(other[, axis, level, fill_value])
- [ ] rank([axis, method, numeric_only, ...])
- [ ] rdiv(other[, axis, level, fill_value])
- [ ] reindex([labels, index, columns, axis, ...])
- [ ] reindex_like(other[, method, copy, limit, ...])
- [ ] rename([mapper, index, columns, axis, copy, ...])
- [ ] rename_axis([mapper, index, columns, axis, ...])
- [ ] reorder_levels(order[, axis])
- [ ] replace([to_replace, value, inplace, limit, ...])
- [ ] resample(rule[, axis, closed, label, ...])
- [ ] reset_index([level, drop, inplace, ...])
- [ ] rfloordiv(other[, axis, level, fill_value])
- [ ] rmod(other[, axis, level, fill_value])
- [ ] rmul(other[, axis, level, fill_value])
- [ ] rolling(window[, min_periods, center, ...])
- [ ] round([decimals])
- [ ] rpow(other[, axis, level, fill_value])
- [ ] rsub(other[, axis, level, fill_value])
- [ ] rtruediv(other[, axis, level, fill_value])
- [ ] sample([n, frac, replace, weights, ...])
- [ ] select_dtypes([include, exclude])
- [ ] sem([axis, skipna, ddof, numeric_only])
- [ ] set_axis(labels, *[, axis, copy])
- [ ] set_flags(*[, copy, allows_duplicate_labels])
- [ ] set_index(keys, *[, drop, append, inplace, ...])
- [ ] shift([periods, freq, axis, fill_value, suffix])
- [ ] skew([axis, skipna, numeric_only])
- [ ] sort_index(*[, axis, level, ascending, ...])
- [ ] sort_values(by, *[, axis, ascending, ...])
- [ ] squeeze([axis])
- [ ] stack([level, dropna, sort, future_stack])
- [ ] std([axis, skipna, ddof, numeric_only])
- [ ] sub(other[, axis, level, fill_value])
- [ ] subtract(other[, axis, level, fill_value])
- [ ] sum([axis, skipna, numeric_only, min_count])
- [ ] swapaxes(axis1, axis2[, copy])
- [ ] swaplevel([i, j, axis])
- [ ] tail([n])
- [ ] take(indices[, axis])
- [ ] to_clipboard(*[, excel, sep])
- [ ] to_csv([path_or_buf, sep, na_rep, ...])
- [ ] to_dict([orient, into, index])
- [ ] to_excel(excel_writer, *[, sheet_name, ...])
- [ ] to_feather(path, **kwargs)
- [ ] to_gbq(destination_table, *[, project_id, ...])
- [ ] to_hdf(path_or_buf, *, key[, mode, ...])
- [ ] to_html([buf, columns, col_space, header, ...])
- [ ] to_json([path_or_buf, orient, date_format, ...])
- [ ] to_latex([buf, columns, header, index, ...])
- [ ] to_markdown([buf, mode, index, storage_options])
- [ ] to_numpy([dtype, copy, na_value])
- [ ] to_orc([path, engine, index, engine_kwargs])
- [ ] to_parquet([path, engine, compression, ...])
- [ ] to_period([freq, axis, copy])
- [ ] to_pickle(path, *[, compression, protocol, ...])
- [ ] to_records([index, column_dtypes, index_dtypes])
- [ ] to_sql(name, con, *[, schema, if_exists, ...])
- [ ] to_stata(path, *[, convert_dates, ...])
- [ ] to_string([buf, columns, col_space, header, ...])
- [ ] to_timestamp([freq, how, axis, copy])
- [ ] to_xarray()
- [ ] to_xml([path_or_buffer, index, root_name, ...])
- [ ] transform(func[, axis])
- [ ] transpose(*args[, copy])
- [ ] truediv(other[, axis, level, fill_value])
- [ ] truncate([before, after, axis, copy])
- [ ] tz_convert(tz[, axis, level, copy])
- [ ] tz_localize(tz[, axis, level, copy, ...])
- [ ] unstack([level, fill_value, sort])
- [ ] update(other[, join, overwrite, ...])
- [ ] value_counts([subset, normalize, sort, ...])
- [ ] var([axis, skipna, ddof, numeric_only])
- [ ] where(cond[, other, inplace, axis, level])
- [ ] xs(key[, axis, level, drop_level])

## Detailed Steps

This document outlines the steps to implement new DataFrame methods. Follow these
steps carefully.

### 1. Understand the Task

- [ ] **Read this document carefully**: Before you begin, read this entire
      document (`specs/2025-10-27-dataframe-methods.md`) to understand the scope
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
- [ ] **Implement the method**: Add the method to `leanframe/core/frame.py`.
      Ensure your implementation is consistent with the existing codebase.
- [ ] **Add unit tests**: Create comprehensive unit tests for the new method in
      `tests/unit/test_frame.py`. Cover edge cases and different data types.

### 3. Verify Your Changes

- [ ] **Run tests**: Execute all tests by running `uv run pytest tests` to
      ensure your changes haven't introduced any regressions.
- [ ] **Run static analysis**: Run `uv run mypy leanframe tests` and `uv run
      ruff check` to check for type errors and linting issues.
- [ ] **Mark the method as complete**: Once the implementation is complete and
      all checks pass, edit this file (`specs/2025-10-27-dataframe-methods.md`) and
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
