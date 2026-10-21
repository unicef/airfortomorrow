from __future__ import annotations

from pathlib import Path

import pandas as pd
from behave import then


ERA5_REALTIME_PREFIX = "data/processed/era5/daily_aggregated/realtime/era5_daily_mean_"


def _require_last_result(context):
    if context.last_result is None:
        raise AssertionError("No command has been executed in this scenario.")
    return context.last_result


def _parse_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _resolve_updated_era5_realtime_parquets(context) -> list[Path]:
    result = _require_last_result(context)
    matches: list[Path] = []

    for rel_path in result.updated_data_files:
        if not rel_path.startswith(ERA5_REALTIME_PREFIX):
            continue
        if not rel_path.endswith(".parquet"):
            continue
        abs_path = context.repo_root / rel_path
        if abs_path.exists() and abs_path.is_file():
            matches.append(abs_path)

    return sorted(matches, key=lambda candidate: candidate.stat().st_mtime_ns)


def _load_latest_era5_parquet(context) -> pd.DataFrame:
    parquet_path = getattr(context, "latest_era5_realtime_parquet", None)
    assert parquet_path is not None, (
        "No ERA5 parquet selected yet. "
        "Run step: 'the current run should produce at least N updated ERA5 realtime parquet files'."
    )
    assert parquet_path.exists(), f"ERA5 parquet does not exist: {parquet_path}"
    assert parquet_path.is_file(), f"ERA5 parquet path is not a file: {parquet_path}"

    cached_path = getattr(context, "_cached_era5_parquet_path", None)
    cached_df = getattr(context, "_cached_era5_parquet_df", None)
    if cached_path == parquet_path and cached_df is not None:
        return cached_df

    df = pd.read_parquet(parquet_path)
    context._cached_era5_parquet_path = parquet_path
    context._cached_era5_parquet_df = df
    return df


@then('the command output should include ERA5 CLI options "{options_csv}"')
def step_output_includes_era5_cli_options(context, options_csv: str):
    result = _require_last_result(context)
    options = _parse_csv(options_csv)
    missing = [option for option in options if option not in result.output]
    assert not missing, (
        f"Missing expected ERA5 CLI options in command output: {missing}\n"
        f"Command output:\n{result.output}"
    )


@then('the current run should produce at least {count:d} updated ERA5 realtime parquet files')
def step_current_run_produces_era5_realtime_parquet(context, count: int):
    matches = _resolve_updated_era5_realtime_parquets(context)
    assert len(matches) >= count, (
        f"Expected at least {count} updated ERA5 realtime parquet files, found {len(matches)}. "
        f"Matched files: {[str(path) for path in matches]}"
    )
    context.latest_era5_realtime_parquet = matches[-1]
    context._cached_era5_parquet_path = None
    context._cached_era5_parquet_df = None
    context.selected_era5_meteorological_column = None


@then('the latest updated ERA5 realtime parquet should contain at least {min_rows:d} rows')
def step_latest_era5_realtime_parquet_min_rows(context, min_rows: int):
    df = _load_latest_era5_parquet(context)
    assert len(df) >= min_rows, (
        f"Expected at least {min_rows} rows in ERA5 parquet, found {len(df)}"
    )


@then('the latest updated ERA5 realtime parquet should include columns "{columns_csv}"')
def step_latest_era5_realtime_parquet_has_columns(context, columns_csv: str):
    df = _load_latest_era5_parquet(context)
    required_columns = _parse_csv(columns_csv)
    missing_columns = [column for column in required_columns if column not in df.columns]
    assert not missing_columns, (
        f"Missing required columns in ERA5 parquet: {missing_columns}. "
        f"Available columns: {list(df.columns)}"
    )


@then(
    'the latest updated ERA5 realtime parquet should include at least one meteorological column from "{columns_csv}"'
)
def step_latest_era5_realtime_parquet_has_meteorological_column(context, columns_csv: str):
    df = _load_latest_era5_parquet(context)
    candidates = _parse_csv(columns_csv)
    selected = next((column for column in candidates if column in df.columns), None)
    assert selected is not None, (
        f"Expected at least one meteorological column from {candidates}, "
        f"available columns are {list(df.columns)}"
    )
    context.selected_era5_meteorological_column = selected


@then('the latest updated ERA5 realtime parquet should have non-null values in columns "{columns_csv}"')
def step_latest_era5_realtime_parquet_non_null_columns(context, columns_csv: str):
    df = _load_latest_era5_parquet(context)
    required_columns = _parse_csv(columns_csv)
    missing_columns = [column for column in required_columns if column not in df.columns]
    assert not missing_columns, (
        f"Cannot validate nulls because required columns are missing: {missing_columns}"
    )

    null_counts = {column: int(df[column].isna().sum()) for column in required_columns}
    bad_columns = {column: count for column, count in null_counts.items() if count > 0}
    assert not bad_columns, (
        f"Expected non-null values in columns {required_columns}, but found nulls: {bad_columns}"
    )


@then("the latest updated ERA5 realtime parquet should have numeric values in the selected meteorological column")
def step_latest_era5_realtime_parquet_numeric_selected_meteorological(context):
    df = _load_latest_era5_parquet(context)
    column = getattr(context, "selected_era5_meteorological_column", None)
    assert column, (
        "No selected ERA5 meteorological column is set. "
        "Run step: 'the latest updated ERA5 realtime parquet should include at least one meteorological column from \"...\"'."
    )
    assert column in df.columns, f"Selected meteorological column {column!r} is missing."
    numeric_series = pd.to_numeric(df[column], errors="coerce")
    invalid_count = int(numeric_series.isna().sum())
    assert invalid_count == 0, (
        f"Expected numeric values in selected meteorological column {column!r}, "
        f"found {invalid_count} non-numeric values"
    )


@then('the latest updated ERA5 realtime parquet should have at least {min_unique:d} unique values in column "{column}"')
def step_latest_era5_realtime_parquet_min_unique(context, min_unique: int, column: str):
    df = _load_latest_era5_parquet(context)
    assert column in df.columns, f"Column {column!r} not found in ERA5 parquet"
    unique_count = int(df[column].nunique(dropna=True))
    assert unique_count >= min_unique, (
        f"Expected at least {min_unique} unique values in {column!r}, found {unique_count}"
    )


@then('the referenced log file should not contain "{text}"')
def step_referenced_log_file_not_contains(context, text: str):
    log_path = getattr(context, "last_referenced_log_path", None)
    assert log_path is not None, (
        "No referenced log file is available. "
        "Run step: 'the command output should reference an existing log file'."
    )
    assert log_path.exists() and log_path.is_file(), f"Referenced log file is invalid: {log_path}"
    content = log_path.read_text(encoding="utf-8", errors="ignore")
    assert text not in content, (
        f"Unexpected text found in referenced log file {log_path}: {text!r}"
    )
