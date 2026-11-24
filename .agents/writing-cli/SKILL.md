---
name: writing-cli
description: Use when adding, renaming, or changing a `valk` command, option, or its output; keeps the noun/verb layout, shared flag names, stdout/stderr split, lazy imports, and old-name aliases consistent.
---

# Writing CLI commands

New commands should read like the ones already there. Check `valk <group> --help` for a sibling before inventing a name.

## Layout

- Groups are nouns (`run`, `agent`, `dataset`, `tracker`) and commands are verbs (`list`, `show`, `start`, `update`, `download`).
- Register commands in the group's `LazyGroup` `lazy_commands` with a one-line help, so `valk --help` imports nothing.
- Prefer one command with a flag over two near-identical commands. `run update --shed` and `run task --links` are the pattern.
- `show` takes one or more IDs and prints details. `list` takes filters and prints a table.

## Shared flags

| Need | Flag |
| --- | --- |
| Machine-readable data | `--json` |
| Streamed records | `--jsonl` |
| Where to write files | `-o, --output` |
| Stream until done | `--follow` |
| Skip a prompt | `-y, --yes` |
| Replace an existing file | `--overwrite` |
| Many IDs | `--task-ids A,B` or `--task-ids @FILE` |
| Filter by name | `--agent`, `--benchmark`, `--model`, `--dataset` |
| Sort | `--order` |

- Use `json_option()` from `cli/display.py` for `--json`/`--jsonl`, and `resolve_task_ids` for task IDs.
- Global flags (`--tracker`, `--verbose`, `--no-color`) live on `valk` itself. Don't add per-command copies.

## Output

- Data goes to stdout. Progress, prompts, warnings, and success messages go to stderr.
- `--json` prints one plain JSON value through `emit` and never prompts, colors, or paginates.
- Tables go through `format_table`, statuses through `status_text`, and prompts through `confirm`.
- One object prints through `details`: a header, aligned rows, then `next:` commands.
- Color text with the roles in `valkyrie/palette.py` through `echo`, `done`, `warn`, and `details`. Never pick a `click.style` color.
- Errors raise `Failure(message, detail=..., hint=command(...))`, which prints `error:`, a dim detail, and a `try:` line.
- Say what failed, not the HTTP status. Status codes and tracebacks show only with `-v`.

## Imports

- Import the SDK, Rich, and heavy modules inside the command function, not at module top level.
- Commands outside `valk tui` never import Textual.

## Renaming

- Add the old name to `cli/legacy.py`. It rewrites the old command or flag to the new one and prints a warning on stderr.
- Keep old names out of help, completion, and docs, and add a row to the table in `tests/unit/cli/test_entry.py`.
- Delete the alias in a later release, not in the same PR as the rename.

## After a change

- Run `make docs-reference` to regenerate `src/valkyrie/cli/completion.json` and the CLI reference.
- Then follow `.agents/writing-tests/SKILL.md` for which suites prove the change.
