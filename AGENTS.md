# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project at a glance
- `moexr` is an unofficial Python client for the MOEX (Moscow Exchange) ISS REST API.
- License: MIT.
- Main goal: keep a clean separation between low-level API transport and optional higher-level integrations.
- Current repo state contains two publishable packages:
  - `moexr-client` (async low-level client)
  - `moexr-pandas` (optional pandas integration)
- Some docs/workflows also reference future or external packages (`moexr`, `moexr-ql`, `moexr-redis`). Do not assume they exist locally unless the directory is present.

## Repository structure
- Workspace root:
  - `pyproject.toml`: uv workspace, shared dev tools, ruff config.
  - `pyrightconfig.json`: strict type-checking rules for `packages/`.
  - `.github/workflows/ci.yml`: lint, type-check, test, and build matrix.
  - `.github/workflows/publish.yml`: tag-based PyPI publishing.
- Packages:
  - `packages/moexr-client/`
    - `src/moexr/client/`: async HTTP client, result models, errors.
    - `tests/`: unit tests + JSON fixtures in `tests/data/`.
  - `packages/moexr-pandas/`
    - `src/moexr/pandas/`: conversion helpers (`MoexTableResult` -> `pandas.DataFrame`).
    - `tests/`: pandas integration tests.

## Architecture and boundaries
- Keep package boundaries strict:
  - `moexr-client` must stay lightweight and focused on transport/result handling.
  - Optional integrations belong in optional packages (`moexr-pandas`, etc.).
- Do not pull heavy dependencies into client/core layers.
- Preserve namespace intent:
  - `moexr.client` for low-level client
  - `moexr.pandas` for pandas helpers

## Stack and tooling
- Python: `>=3.11` (CI tests 3.11-3.14).
- Package/build manager: `uv` only.
- Test runner: `pytest`.
- Lint/format checks: `ruff`.
- Type checking: `pyright` (strict, configured at repo root).
- Async HTTP in client: `aiohttp`.

## Standard workflow for agent changes
1. Pick the correct package and keep changes local to it.
2. Implement minimal typed changes.
3. Add/update tests in the same package.
4. Run validation for touched package(s):
   - Install/sync: `uv sync --locked`
   - Tests: `uv run pytest -q`
   - Lint: `uv run ruff check`
   - Types: `uv run pyright .`
5. Build sanity check when relevant:
   - `uv build --package moexr-client`
   - `uv build --package moexr-pandas`

## Coding expectations
- Follow PEP 8 and existing local style (ruff line length 100, single quotes).
- Prefer explicit types and keep public API behavior predictable.
- Keep functions small/composable.
- Avoid hidden coupling across packages.
- For network-related behavior, preserve deterministic tests (fixtures/mocking over live calls).

## Dependency rules
- Add dependencies only in the package that needs them.
- Avoid turning optional capabilities into mandatory dependencies.
- If adding a dependency, confirm it does not leak into unrelated packages.

## CI and release notes
- CI validates each package independently (lint, pyright, pytest, build).
- Publishing is tag-driven (`<package>-vX.Y.Z`) via GitHub Actions + PyPI Trusted Publishing.
- If changing public API, ensure docs/versioning are updated in the affected package.

## Practical guardrails
- Prefer edits that match existing module layout and naming.
- Update package README/tests when behavior changes.
- If instructions conflict, trust actual repository state and CI configuration first, then align docs.
