# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project at a glance
- `moexr` is an unofficial Python client for the MOEX (Moscow Exchange) ISS REST API.
- License: MIT.
- Main goal: keep a clean separation between low-level API transport and optional higher-level integrations.
- Current repo state contains three publishable packages:
  - `moexr-client` (async low-level client)
  - `moexr` (high-level query API wrapping `moexr-client`)
  - `moexr-pandas` (optional pandas integration)
- All packages share the `moexr` namespace via PEP 420 implicit namespace packages (no `__init__.py` in namespace dirs).

## Repository structure
- Workspace root:
  - `pyproject.toml`: uv workspace, shared dev tools, ruff config.
  - `pyrightconfig.json`: shared strict type-checking rules (inherited by per-package configs).
  - `.github/workflows/ci.yml`: lint, type-check, test, and build matrix.
  - `.github/workflows/publish.yml`: tag-based PyPI publishing.
  - `fixtures/`: shared JSON fixtures used by package tests.
- Packages:
  - `packages/moexr-client/`
    - `src/moexr/client/`: async HTTP client, table models (`MoexTable`, `MoexIndexedTable`), errors.
    - `tests/`: unit tests that use shared JSON fixtures from `fixtures/`.
  - `packages/moexr/`
    - `src/moexr/query/`: high-level query API (`Moex` class, `AutoPagination`).
    - `tests/`: unit tests with mocked client.
  - `packages/moexr-pandas/`
    - `src/moexr/pandas/`: conversion helpers (`MoexTable`/`MoexIndexedTable` -> `pandas.DataFrame`).
    - `tests/`: pandas integration tests.

## Namespace package architecture
All packages contribute sub-modules under the shared `moexr` namespace using PEP 420 implicit namespace packages:
- `moexr.client` (from `moexr-client`)
- `moexr.query` (from `moexr`)
- `moexr.pandas` (from `moexr-pandas`)

**Important**: The `moexr/` directory in each package's `src/` must NOT contain `__init__.py`. Only the leaf sub-module directories (e.g., `client/`, `query/`, `pandas/`) have `__init__.py`. This is required for pyright compatibility and correct namespace package behaviour.

## Architecture and boundaries
- Keep package boundaries strict:
  - `moexr-client` must stay lightweight and focused on transport/result handling.
  - Optional integrations belong in optional packages (`moexr-pandas`, etc.).
- Do not pull heavy dependencies into client/core layers.
- Preserve namespace intent:
  - `moexr.client` for low-level client
  - `moexr.query` for high-level query API
  - `moexr.pandas` for pandas helpers

## Stack and tooling
- Python: `>=3.11` (CI tests 3.11-3.14).
- Package/build manager: `uv` only.
- Test runner: `pytest`.
- Lint/format checks: `ruff`.
- Type checking: `pyright` (strict, per-package configs extending root).
- Async HTTP in client: `aiohttp`.

## Pyright configuration
- **Root** `pyrightconfig.json`: shared strict settings only (no `include`/`extraPaths`).
- **Per-package** `pyrightconfig.json` in each `packages/<pkg>/`: extends root, sets `include: ["."]` and `extraPaths` pointing to its own `src/` plus any workspace dependency source roots.
- Always run pyright **from a package directory** (e.g., `packages/moexr-client/`), not from the repo root. This ensures the per-package config is used.

## Standard workflow for agent changes
1. Pick the correct package and keep changes local to it.
2. Implement minimal typed changes.
3. Add/update tests in the same package.
4. Run validation for touched package(s) from the package directory:
   ```bash
   # First, ensure deps are synced (run once from repo root):
   uv sync --locked --all-packages

   # Then validate each touched package (from its directory):
   cd packages/<pkg>
   uv run pyright .
   uv run ruff check
   uv run pytest -q
   ```
5. Build sanity check when relevant (from repo root):
   ```bash
   uv build --package moexr-client
   uv build --package moexr
   uv build --package moexr-pandas
   ```

## Coding expectations
- Follow PEP 8 and existing local style (ruff line length 140, double quotes).
- Prefer explicit types and keep public API behavior predictable.
- Keep functions small/composable.
- Avoid hidden coupling across packages.
- For network-related behavior, preserve deterministic tests (fixtures/mocking over live calls).
- All `__init__.py` files that re-export symbols must define `__all__` (required by pyright strict mode).

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
