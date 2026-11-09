# Development

You need Python 3.12, [uv](https://docs.astral.sh/uv/), and Docker.

## Install from source

From the repository root:

```bash
make install

# Put an editable `valkyrie` on your PATH
make tool-install
```

Run `uv tool update-shell` if your shell can't find `valkyrie` afterwards.

## Check your changes

```bash
# ruff format and ruff check --fix
make style

# basedpyright in strict mode
make typecheck

# CLI, TUI, SDK, contract, docs and local integration tests
make test
```

## Client layout

- `packages/valkyrie-sdk` is the standalone SDK. It never imports `tracker` or `valkyrie.cli`.
- The SDK's wire models are generated from `services/tracker/openapi.json` by `make sdk-models`. After a Tracker
  model change, regenerate the spec with `uv run python generate_openapi.py` in `services/tracker` first.
  `tests/unit/sdk/test_generated_models.py` fails when they are out of date with the spec.
- The TUI reads runs only through `RunSource` in `src/valkyrie/tui/source.py` and never imports
  `valkyrie.cli`.
- CLI commands reach the Tracker through `call()` in `src/valkyrie/cli/client.py`, which runs an
  SDK `ValkyrieClient`. Account routes such as login, access keys and organizations go through
  `client.account`.
- The CLI and TUI import no `httpx`, and take SDK code only from `valkyrie.sdk`, `valkyrie.sdk.models`
  and `valkyrie.sdk.errors`. Export anything else they need from one of those modules.

`tests/unit/sdk/test_import_boundaries.py` fails when the SDK imports the Tracker, CLI or TUI, when
the TUI imports the CLI, or when the CLI or TUI breaks the import rules above.

## Work on other parts

- The Tracker service, its tests and migrations: [services/tracker/README.md](services/tracker/README.md)
- The documentation site: [docs/README.md](docs/README.md)
- Running benchmarks on your machine without AWS:
  [Run Tracker locally](https://docs.valkyrie.vals.ai/self-hosting/local-tracker)
