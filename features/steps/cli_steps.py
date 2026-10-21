from __future__ import annotations

import importlib
import os
from typing import Iterable

from behave import given, then, when
from behave.configuration import Configuration


def _require_last_result(context):
    if context.last_result is None:
        raise AssertionError("No command has been executed in this scenario.")
    return context.last_result


def _parse_exit_codes(raw_codes: str) -> Iterable[int]:
    return [int(value.strip()) for value in raw_codes.split(",") if value.strip()]


@then('default test selection for tags "{tags}" should be "{selected}"')
def step_default_test_selection(context, tags: str, selected: str):
    actual = Configuration([]).tags.check(tags.split())
    assert actual == (selected == "yes"), (
        f"Default suite selection for {tags!r}: expected {selected}, got {actual}"
    )


@given('the environment variable "{name}" should equal "{value}"')
def step_env_var_equals(context, name: str, value: str):
    actual = os.getenv(name)
    assert actual == value, f"Expected {name}={value!r}, got {actual!r}"


@given('python module "{module_name}" should import successfully')
def step_python_module_import(context, module_name: str):
    module = importlib.import_module(module_name)
    assert module is not None


@given('python attribute "{dotted_path}" should exist')
def step_python_attribute_exists(context, dotted_path: str):
    module_name, attribute = dotted_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    assert hasattr(module, attribute), f"Missing attribute: {dotted_path}"


@when('I run "{command}"')
def step_run_command(context, command: str):
    context.run_shell(command)


@when('I run "{command}" with timeout {timeout:d}')
def step_run_command_with_timeout(context, command: str, timeout: int):
    context.run_shell(command, timeout=timeout)


@then("the command should succeed")
def step_command_should_succeed(context):
    result = _require_last_result(context)
    if getattr(result, "timed_out", False):
        raise AssertionError(
            f"Command timed out after {result.timeout_seconds}s.\n{result.output}"
        )
    assert result.returncode == 0, result.output


@then("the command should fail")
def step_command_should_fail(context):
    result = _require_last_result(context)
    assert result.returncode != 0, result.output


@then("the command exit code should be {code:d}")
def step_command_exit_code(context, code: int):
    result = _require_last_result(context)
    assert result.returncode == code, result.output


@then('the command exit code should be one of "{codes}"')
def step_command_exit_code_one_of(context, codes: str):
    result = _require_last_result(context)
    allowed = set(_parse_exit_codes(codes))
    assert result.returncode in allowed, (
        f"Expected return code in {sorted(allowed)}, got {result.returncode}.\n"
        f"{result.output}"
    )


@then('the command output should contain "{text}"')
def step_command_output_contains(context, text: str):
    result = _require_last_result(context)
    assert text in result.output, result.output


@then('the command output should not contain "{text}"')
def step_command_output_not_contains(context, text: str):
    result = _require_last_result(context)
    assert text not in result.output, result.output


@then('the command output should contain "{text}" case-insensitively')
def step_command_output_contains_ci(context, text: str):
    result = _require_last_result(context)
    assert text.lower() in result.output.lower(), result.output
