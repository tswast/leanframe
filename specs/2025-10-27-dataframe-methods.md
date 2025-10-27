# DataFrame methods

Implement scalar, aggregate, and window methods for leanframe DataFrame objects.

## Background

The leanframe package aims to provide a pandas-compatible interface while maintaining a 1:1 mapping to related Ibis types. DataFrame is 1:1 with Ibis Table, and Series is 1:1 with Ibis Column.

Pandas features not supported by Ibis, such as specifying index column(s) or a total ordering over all rows are also not supported by leanframe.

## Acceptance Criteria

Implement each pandas DataFrame method that doesn't require an index or ordering. When an item is completed, edit specs/2025-10-27-dataframe-methods.md and check off the item with an x.

### Attributes

- [x] T -- not feasible, requires index
- [x] at -- not feasible, requires index
- [ ] attrs
- [x] axes -- not feasible, requires index
- [ ] columns
- [ ] dtypes
- [ ] empty
- [ ] flags
- [x] iat -- not feasible, requires index
- [x] iloc -- not feasible, requires index
- [x] index -- not feasible, requires index
- [x] loc -- not feasible, requires index
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
- [x] align(other[, join, axis, level, copy, ...]) -- not feasible, requires index
- [ ] all([axis, bool_only, skipna])
- [ ] any(*[, axis, bool_only, skipna])
- [ ] apply(func[, axis, raw, result_type, args, ...])
- [ ] applymap(func[, na_action])
- [ ] asfreq(freq[, method, how, normalize, ...])
- [ ] asof(where[, subset])
- [ ] assign(**kwargs)
- [ ] astype(dtype[, copy, errors])
- [x] at_time(time[, asof, axis]) -- not feasible, requires index
- [ ] backfill(*[, axis, inplace, limit, downcast])
- [x] between_time(start_time, end_time[, ...]) -- not feasible, requires index
- [ ] bfill(*[, axis, inplace, limit, limit_area, ...])
- [ ] bool()
- [ ] boxplot([column, by, ax, fontsize, rot, ...])
- [ ] clip([lower, upper, axis, inplace])
- [ ] combine(other, func[, fill_value, overwrite])
- [ ] combine_first(other)
- [x] compare(other[, align_axis, keep_shape, ...]) -- not feasible, requires index
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
- [x] droplevel(level[, axis]) -- not feasible, requires index
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
- [x] first_valid_index() -- not feasible, requires index
- [ ] floordiv(other[, axis, level, fill_value])
- [x] from_dict(data[, orient, dtype, columns]) -- not feasible, requires index
- [x] from_records(data[, index, exclude, ...]) -- not feasible, requires index
- [ ] ge(other[, axis, level])
- [ ] get(key[, default])
- [ ] groupby([by, axis, level, as_index, sort, ...])
- [ ] gt(other[, axis, level])
- [ ] head([n])
- [ ] hist([column, by, grid, xlabelsize, xrot, ...])
- [x] idxmax([axis, skipna, numeric_only]) -- not feasible, requires index
- [x] idxmin([axis, skipna, numeric_only]) -- not feasible, requires index
- [ ] infer_objects([copy])
- [ ] info([verbose, buf, max_cols, memory_usage, ...])
- [ ] insert(loc, column, value[, allow_duplicates])
- [ ] interpolate([method, axis, limit, inplace, ...])
- [x] isetitem(loc, value) -- not feasible, requires index
- [ ] isin(values)
- [ ] isna()
- [ ] isnull()
- [ ] items()
- [x] iterrows() -- not feasible, requires index
- [x] itertuples([index, name]) -- not feasible, requires index
- [ ] join(other[, on, how, lsuffix, rsuffix, ...])
- [ ] keys()
- [ ] kurt([axis, skipna, numeric_only])
- [ ] kurtosis([axis, skipna, numeric_only])
- [ ] last(offset)
- [x] last_valid_index() -- not feasible, requires index
- [ ] le(other[, axis, level])
- [ ] lt(other[, axis, level])
- [ ] map(func[, na_action])
- [ ] mask(cond[, other, inplace, axis, level])
- [ ] max([axis, skipna, numeric_only])
- [ ] mean([axis, skipna, numeric_only])
- [ ] median([axis, skipna, numeric_only])
- [ ] melt([id_vars, value_vars, var_name, ...])
- [x] memory_usage([index, deep]) -- not feasible, requires index
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
- [x] pivot(*, columns[, index, values]) -- not feasible, requires index
- [x] pivot_table([values, index, columns, ...]) -- not feasible, requires index
- [ ] pop(item)
- [ ] pow(other[, axis, level, fill_value])
- [ ] prod([axis, skipna, numeric_only, min_count])
- [ ] product([axis, skipna, numeric_only, min_count])
- [ ] quantile([q, axis, numeric_only, ...])
- [ ] query(expr, *[, inplace])
- [ ] radd(other[, axis, level, fill_value])
- [ ] rank([axis, method, numeric_only, ...])
- [ ] rdiv(other[, axis, level, fill_value])
- [x] reindex([labels, index, columns, axis, ...]) -- not feasible, requires index
- [x] reindex_like(other[, method, copy, limit, ...]) -- not feasible, requires index
- [ ] rename([mapper, index, columns, axis, copy, ...])
- [x] rename_axis([mapper, index, columns, axis, ...]) -- not feasible, requires index
- [x] reorder_levels(order[, axis]) -- not feasible, requires index
- [ ] replace([to_replace, value, inplace, limit, ...])
- [ ] resample(rule[, axis, closed, label, ...])
- [x] reset_index([level, drop, inplace, ...]) -- not feasible, requires index
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
- [x] set_axis(labels, *[, axis, copy]) -- not feasible, requires index
- [ ] set_flags(*[, copy, allows_duplicate_labels])
- [x] set_index(keys, *[, drop, append, inplace, ...]) -- not feasible, requires index
- [ ] shift([periods, freq, axis, fill_value, suffix])
- [ ] skew([axis, skipna, numeric_only])
- [x] sort_index(*[, axis, level, ascending, ...]) -- not feasible, requires index
- [ ] sort_values(by, *[, axis, ascending, ...])
- [ ] squeeze([axis])
- [x] stack([level, dropna, sort, future_stack]) -- not feasible, requires index
- [ ] std([axis, skipna, ddof, numeric_only])
- [ ] sub(other[, axis, level, fill_value])
- [ ] subtract(other[, axis, level, fill_value])
- [ ] sum([axis, skipna, numeric_only, min_count])
- [x] swapaxes(axis1, axis2[, copy]) -- not feasible, requires index
- [x] swaplevel([i, j, axis]) -- not feasible, requires index
- [ ] tail([n])
- [x] take(indices[, axis]) -- not feasible, requires index
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
- [x] transpose(*args[, copy]) -- not feasible, requires index
- [ ] truediv(other[, axis, level, fill_value])
- [x] truncate([before, after, axis, copy]) -- not feasible, requires index
- [x] tz_convert(tz[, axis, level, copy]) -- not feasible, requires index
- [x] tz_localize(tz[, axis, level, copy, ...]) -- not feasible, requires index
- [x] unstack([level, fill_value, sort]) -- not feasible, requires index
- [x] update(other[, join, overwrite, ...]) -- not feasible, requires index
- [ ] value_counts([subset, normalize, sort, ...])
- [ ] var([axis, skipna, ddof, numeric_only])
- [ ] where(cond[, other, inplace, axis, level])
- [x] xs(key[, axis, level, drop_level]) -- not feasible, requires index

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
