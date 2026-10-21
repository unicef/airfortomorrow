# Contributing

Please read and follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before participating.

## Development Principles

1. Never commit secrets, tokens, or private credentials.
2. Keep changes focused and reviewable.
3. Prefer explicit, argument-safe process execution over shell-string evaluation.
4. Keep generated artifacts out of source control.

## Branching

1. Create feature branches from `main`.
2. Open pull requests with clear scope and test evidence.
3. Keep unrelated refactors out of the same PR.

## Testing

1. Run BDD tests with `./scripts/run_behave_tests.sh`.
2. Default runs exclude `@slow` and `@live`; use tags when needed (for example `./scripts/run_behave_tests.sh -- --tags=@contract`).
3. Run `./scripts/smoke_test.sh` when you need to validate a fresh Docker build plus `@smoke` scenarios.
4. Do not run test suites directly on the host; use Docker harness scripts.

## Security and Data Hygiene

1. Do not commit `.env` files or credentials.
2. Do not commit logs, caches, temporary files, or bytecode artifacts.
3. Strip notebook outputs and local path traces before commit.
4. Follow Git LFS policy for approved large data artifacts.

## Pull Request Checklist

1. Code changes are limited to the intended scope.
2. No secrets or personal local paths were introduced.
3. CI checks pass.
4. Documentation is updated when behavior changes.
5. Security implications were considered for process execution and credential handling.

## Commit Messages

Use short, imperative messages describing what changed and why.
