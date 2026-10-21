from __future__ import annotations

import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

from behave import given, then, when


REQUIRED_ENV_KEYS = [
    "HIMAWARI_FTP_USER",
    "HIMAWARI_FTP_PASSWORD",
    "CDSAPI_URL",
    "CDSAPI_KEY",
    "OPENAQ_API_KEY",
]

COMPONENT_LOG_PREFIXES = ("openaq_realtime_", "integrated_realtime_pipeline_")

AUTH_FAILURE_PATTERNS = [
    re.compile(r"Invalid credentials", re.IGNORECASE),
    re.compile(r"\bHTTP\s*401\b", re.IGNORECASE),
    re.compile(r"\bHTTP\s*403\b", re.IGNORECASE),
    re.compile(r"NotAuthorized", re.IGNORECASE),
    re.compile(r"Login incorrect", re.IGNORECASE),
    re.compile(r"FTP username and password must be provided", re.IGNORECASE),
]

PRESERVED_TRACKED_DATA_FILES = {
    "data/training/silver_dataset.parquet",
}

PRESERVED_TRACKED_DATA_PREFIXES = (
    "data/raw/firms/historical/",
)

ERROR_LOG_MARKERS = [
    re.compile(r"\bERROR\b"),
    re.compile(r"Traceback \(most recent call last\)"),
]


def _resolve_path(context, raw_path: str) -> Path:
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    return context.repo_root / candidate


def _is_preserved_tracked_data_path(repo_relative_path: str) -> bool:
    if repo_relative_path in PRESERVED_TRACKED_DATA_FILES:
        return True
    return any(
        repo_relative_path.startswith(prefix)
        for prefix in PRESERVED_TRACKED_DATA_PREFIXES
    )


@given('the file "{path}" should exist')
def step_file_should_exist(context, path: str):
    resolved = _resolve_path(context, path)
    assert resolved.exists(), f"Path does not exist: {resolved}"
    assert resolved.is_file(), f"Path is not a file: {resolved}"


@then('the file "{path}" should contain "{text}"')
def step_file_should_contain(context, path: str, text: str):
    resolved = _resolve_path(context, path)
    assert resolved.exists(), f"Path does not exist: {resolved}"
    content = resolved.read_text(encoding="utf-8")
    assert text in content, f"Text not found in {resolved}: {text}"


@given("I create a temporary quickstart workspace")
def step_create_temp_quickstart_workspace(context):
    workspace = Path(tempfile.mkdtemp(prefix="air_quality_quickstart_"))
    context.quickstart_workspace = workspace


@given('I prepare realtime silver input for countries "{countries}" for today')
def step_prepare_realtime_silver_for_today(context, countries: str):
    country_codes = sorted(code.strip() for code in countries.split() if code.strip())
    assert country_codes, "No country codes were provided."

    silver_dir = context.repo_root / "data" / "silver" / "realtime"
    assert silver_dir.exists(), f"Silver directory does not exist: {silver_dir}"

    countries_key = "_".join(country_codes)
    source_candidates = sorted(
        silver_dir.glob(f"silver_realtime_{countries_key}_*.parquet"),
        key=lambda candidate: candidate.stat().st_mtime_ns,
        reverse=True,
    )
    assert source_candidates, (
        f"No realtime silver files found for countries {countries_key} in {silver_dir}"
    )

    source_path = source_candidates[0]
    today_str = datetime.now().strftime("%Y%m%d")
    target_path = silver_dir / f"silver_realtime_{countries_key}_{today_str}.parquet"

    if source_path != target_path:
        shutil.copy2(source_path, target_path)
    assert target_path.exists(), f"Prepared silver file does not exist: {target_path}"


@when('I copy "{source}" to "{destination}" in the temporary quickstart workspace')
def step_copy_to_temp_quickstart_workspace(context, source: str, destination: str):
    source_path = _resolve_path(context, source)
    destination_path = context.quickstart_workspace / destination
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination_path)


@then('the path "{path}" in the temporary quickstart workspace should be a regular file')
def step_temp_workspace_file(context, path: str):
    resolved = context.quickstart_workspace / path
    assert resolved.exists(), f"Path does not exist: {resolved}"
    assert resolved.is_file(), f"Path is not a file: {resolved}"


@then('the temporary quickstart ".env" file should contain required credential keys')
def step_temp_workspace_env_contains_required_keys(context):
    env_path = context.quickstart_workspace / ".env"
    content = env_path.read_text(encoding="utf-8")
    missing_keys = [key for key in REQUIRED_ENV_KEYS if key not in content]
    assert not missing_keys, f"Missing keys in {env_path}: {missing_keys}"


@given('I clean the "{path}" directory while preserving git-tracked files')
def step_clean_directory_preserving_tracked(context, path: str):
    root = _resolve_path(context, path)
    assert root.exists(), f"Directory does not exist: {root}"
    assert root.is_dir(), f"Path is not a directory: {root}"

    for file_path in root.rglob("*"):
        if not (file_path.is_file() or file_path.is_symlink()):
            continue
        rel_path = file_path.relative_to(context.repo_root).as_posix()
        if not _is_preserved_tracked_data_path(rel_path):
            file_path.unlink()

    for dir_path in sorted(
        (candidate for candidate in root.rglob("*") if candidate.is_dir()),
        key=lambda candidate: len(candidate.parts),
        reverse=True,
    ):
        if any(dir_path.iterdir()):
            continue
        dir_path.rmdir()


@then("the command output should reference an existing log file")
def step_command_output_has_log_file(context):
    result = context.last_result
    assert result is not None, "No command has been executed in this scenario."
    match = re.search(r"Log file:\s*(.+)", result.output)
    assert match is not None, f"No log file reference found.\n{result.output}"
    log_path_text = match.group(1).strip()
    log_path = _resolve_path(context, log_path_text)
    assert log_path.exists(), f"Referenced log file does not exist: {log_path}"
    assert log_path.is_file(), f"Referenced log path is not a file: {log_path}"
    context.last_referenced_log_path = log_path


@then("the current run should generate OpenAQ and Himawari component logs")
def step_current_run_has_component_logs(context):
    result = context.last_result
    assert result is not None, "No command has been executed in this scenario."
    assert result.new_log_files, "No new log files were created during this command."

    new_log_names = [Path(log_path).name for log_path in result.new_log_files]
    missing_prefixes = [
        prefix
        for prefix in COMPONENT_LOG_PREFIXES
        if not any(name.startswith(prefix) for name in new_log_names)
    ]
    assert not missing_prefixes, (
        "Expected component logs were not created. "
        f"Missing prefixes: {missing_prefixes}. "
        f"New logs: {new_log_names}"
    )


@then('the current run logs should contain "{text}"')
def step_current_run_logs_contain(context, text: str):
    result = context.last_result
    assert result is not None, "No command has been executed in this scenario."
    assert any(
        text in _resolve_path(context, path).read_text(encoding="utf-8", errors="replace")
        for path in result.new_log_files
    ), f"Text {text!r} not found in current run logs: {result.new_log_files}"


@then("the current run logs should not contain authentication or credential failures")
def step_current_run_logs_no_auth_failures(context):
    result = context.last_result
    assert result is not None, "No command has been executed in this scenario."

    candidate_logs = [
        _resolve_path(context, relative_log_path)
        for relative_log_path in result.new_log_files
    ]

    referenced_log_path = getattr(context, "last_referenced_log_path", None)
    if referenced_log_path is not None and referenced_log_path not in candidate_logs:
        candidate_logs.append(referenced_log_path)

    assert candidate_logs, "No candidate log files found for authentication checks."

    failures = []
    for log_path in candidate_logs:
        if not log_path.exists() or not log_path.is_file():
            continue
        content = log_path.read_text(encoding="utf-8", errors="ignore")
        for pattern in AUTH_FAILURE_PATTERNS:
            match = pattern.search(content)
            if match:
                failures.append(
                    f"{log_path}: matched '{pattern.pattern}' around '{match.group(0)}'"
                )
                break

    assert not failures, (
        "Detected authentication/credential failures in logs for this run:\n"
        + "\n".join(failures)
    )


@then("the referenced log file should not contain unexpected error markers")
def step_referenced_log_has_no_error_markers(context):
    log_path = getattr(context, "last_referenced_log_path", None)
    assert log_path is not None, "No referenced log path is available in scenario context."
    assert log_path.exists(), f"Referenced log does not exist: {log_path}"
    assert log_path.is_file(), f"Referenced log is not a file: {log_path}"

    matched_lines = []
    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for line_number, line in enumerate(lines, start=1):
        for marker in ERROR_LOG_MARKERS:
            if marker.search(line):
                matched_lines.append(f"{line_number}: {line}")
                break

    assert not matched_lines, (
        f"Unexpected error markers found in referenced log {log_path}:\n"
        + "\n".join(matched_lines[:20])
    )


@then('the directory "{path}" should contain at least {count:d} files matching "{pattern}"')
def step_directory_contains_matching_files(context, path: str, count: int, pattern: str):
    directory = _resolve_path(context, path)
    assert directory.exists(), f"Directory does not exist: {directory}"
    assert directory.is_dir(), f"Path is not a directory: {directory}"
    matches = list(directory.glob(pattern))
    assert len(matches) >= count, (
        f"Expected at least {count} files matching {pattern} in {directory}, "
        f"found {len(matches)}"
    )


@then('the directory "{path}" should contain at least {count:d} files matching "{pattern}" updated by the current command')
def step_directory_contains_fresh_matching_files(context, path: str, count: int, pattern: str):
    result = context.last_result
    assert result is not None, "No command has been executed in this scenario."

    directory = _resolve_path(context, path)
    assert directory.exists(), f"Directory does not exist: {directory}"
    assert directory.is_dir(), f"Path is not a directory: {directory}"

    updated_paths = set(result.updated_data_files)
    all_matches = [candidate for candidate in directory.glob(pattern) if candidate.is_file()]
    fresh_matches = [
        candidate.relative_to(context.repo_root).as_posix()
        for candidate in all_matches
        if candidate.relative_to(context.repo_root).as_posix() in updated_paths
    ]

    assert len(fresh_matches) >= count, (
        f"Expected at least {count} files matching {pattern} in {directory} "
        f"updated by the current command, found {len(fresh_matches)}. "
        f"Fresh matches: {fresh_matches}"
    )


@then('the directory "{path}" should contain at least {count:d} untracked files matching "{pattern}"')
def step_directory_contains_untracked_matching_files(
    context, path: str, count: int, pattern: str
):
    directory = _resolve_path(context, path)
    assert directory.exists(), f"Directory does not exist: {directory}"
    assert directory.is_dir(), f"Path is not a directory: {directory}"
    all_matches = [candidate for candidate in directory.glob(pattern) if candidate.is_file()]

    untracked_matches = []
    tracked_matches = []
    for candidate in all_matches:
        rel_path = candidate.relative_to(context.repo_root).as_posix()
        if _is_preserved_tracked_data_path(rel_path):
            tracked_matches.append(rel_path)
        else:
            untracked_matches.append(rel_path)

    assert len(untracked_matches) >= count, (
        f"Expected at least {count} untracked files matching {pattern} in {directory}, "
        f"found {len(untracked_matches)} untracked and {len(tracked_matches)} tracked matches. "
        f"Tracked matches: {tracked_matches}"
    )
