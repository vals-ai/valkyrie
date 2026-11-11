# Contributing to Valkyrie

To report a bug or request a feature, email [contact@vals.ai](mailto:contact@vals.ai).
Include:

- The exact command or SDK call you ran.
- What you expected, what happened, and the full error output.
- Your CLI version from `uv tool list` and, if you use the SDK, its version from
  `uv pip show valkyrie-sdk`.
- Whether you use hosted Valkyrie or a local Tracker.
- The run ID, if the problem involves a run.

## Set up your environment

You need Python 3.12, [uv](https://docs.astral.sh/uv/), and Docker. From the repository
root:

```bash
make install

# Put an editable `valkyrie` on your PATH
make tool-install
```

If your shell can't find `valkyrie` afterwards, run:

```bash
uv tool update-shell
```

To run benchmarks on your machine without AWS, see
[Run Tracker locally](https://docs.valkyrie.vals.ai/self-hosting/local-tracker).

## Project layout

| Path | Contents | Guide |
| --- | --- | --- |
| `src/valkyrie` | The CLI and TUI | This guide |
| `packages/valkyrie-sdk` | The Python SDK, published to PyPI as `valkyrie-sdk` | This guide |
| `services/tracker` | The Tracker, the server the CLI and SDK talk to | [services/tracker/README.md](services/tracker/README.md) |
| `docs` | The site at [docs.valkyrie.vals.ai](https://docs.valkyrie.vals.ai) | [docs/README.md](docs/README.md) |

## Make a change

- Keep each pull request to one concern.
- Add or update tests for the behavior you change. A bug fix comes with a test that fails
  without it.
- Python files carry no comments or docstrings. Document commands with Click `help=`,
  routes with FastAPI `description=`, and tests with `@pytest.mark.description(...)`.
  - Public SDK docstrings stay because they are the published SDK reference.
  - [AGENTS.md](AGENTS.md) has the full writing rules.
- Never edit generated files by hand.
- A change under `packages/valkyrie-sdk` needs an SDK version bump.

To regenerate the SDK models after a Tracker model change:

```bash
cd services/tracker && uv run python generate_openapi.py && cd ../..

make sdk-models
```

To regenerate the CLI and SDK reference after a public CLI or SDK change:

```bash
make docs-reference
```

To bump the SDK version:

```bash
uv version --package valkyrie-sdk --bump patch

uv lock
```

### Import boundaries

- `packages/valkyrie-sdk` never imports `tracker`, `valkyrie.cli`, or `valkyrie.tui`.
- The TUI reads runs only through `RunSource` in `src/valkyrie/tui/source.py` and never
  imports `valkyrie.cli`.
- CLI commands reach the Tracker through `call()` in `src/valkyrie/cli/client.py`, which
  runs an SDK `ValkyrieClient`. Account routes, such as sign-in, access keys, and
  organizations, go through `client.account`.
- The CLI and TUI never import `httpx`. They take SDK code only from `valkyrie.sdk`,
  `valkyrie.sdk.models`, and `valkyrie.sdk.errors`.
  - Export anything else they need from one of those modules.

`tests/unit/sdk/test_import_boundaries.py` enforces these rules, and
`tests/unit/sdk/test_generated_models.py` fails when the SDK models are out of date with
the Tracker spec.

## Check your changes

From the repository root:

```bash
# ruff format and ruff check --fix
make style

# basedpyright in strict mode
make typecheck

# CLI, TUI, SDK, contract, docs, and local integration tests
make test
```

Tracker changes also run the suites in
[services/tracker/README.md](services/tracker/README.md). Documentation changes run the
checks in [docs/README.md](docs/README.md).

## Open a pull request

- Branch from `prod` and open the pull request against `prod`.
- Title it `type(scope): summary`, for example
  `fix(tracker): retry a failed sandbox delete` or `docs: fix the quickstart link`.
- Fill in [the pull request template](.github/PULL_REQUEST_TEMPLATE.md).

## License

Valkyrie is licensed under the [GNU Affero General Public License v3.0](LICENSE). By
contributing, you agree that your contributions are licensed under the same license.
