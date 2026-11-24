_COMMANDS = {
    "fetch": ["show"],
    "status": ["show"],
    "output": ["download"],
    "outputs": ["download"],
    "artifacts": ["files"],
    "task-artifacts": ["task", "--links"],
    "retag": ["update"],
    "shed": ["update", "--shed"],
}
_FLAGS = {
    "--connect": "--follow",
    "--agent-name": "--agent",
    "--benchmark-name": "--benchmark",
    "--order-by": "--order",
    "--output-dir": "--output",
}
_COMMAND_FLAGS = {"start": {"-n": "--count"}, "results": {"--path": "--output"}}
_POSITIONAL_FLAGS = {"status": "--ids", "artifacts": "--prefix"}
_FORMATS = {"json": ["--json"], "jsonl": ["--jsonl"], "text": []}
_VALUE_FLAGS = {"--format", "--task-ids-file", "--ids", "--prefix", "--path", *_FLAGS}


def _run_command_index(args: list[str]) -> int | None:
    index = 0
    while index < len(args) and args[index].startswith("-"):
        index += 2 if args[index] == "--tracker" else 1
    return index + 1 if index + 1 < len(args) and args[index] == "run" else None


def _split_values(args: list[str]) -> list[str]:
    split: list[str] = []
    for token in args:
        name, equals, value = token.partition("=")
        split += [name, value] if equals and name in _VALUE_FLAGS else [token]
    return split


def upgrade_args(args: list[str]) -> tuple[list[str], list[str]]:
    end = args.index("--") if "--" in args else len(args)
    head, tail = _split_values(args[:end]), args[end:]
    command_index = _run_command_index(head)
    command = head[command_index] if command_index is not None else ""
    flags = _FLAGS | _COMMAND_FLAGS.get(command, {})
    notes: list[str] = []
    upgraded: list[str] = []
    trailing: list[str] = []
    tokens = iter(enumerate(head))
    for index, token in tokens:
        if index == command_index and command in _COMMANDS:
            notes.append(f"run {command} is now run {' '.join(_COMMANDS[command])}")
            new_command, *trailing = _COMMANDS[command]
            upgraded.append(new_command)
        elif command == "start" and token.startswith("-n") and token[2:].isdigit():
            notes.append("-n is now --count")
            upgraded += ["--count", token[2:]]
        elif token in flags:
            notes.append(f"{token} is now {flags[token]}")
            upgraded.append(flags[token])
        elif token == "--format":
            value = next(tokens, (0, ""))[1]
            replacement = _FORMATS.get(value.lower())
            notes.append(f"--format {value} is now {' '.join(replacement or []) or 'the default'}")
            upgraded += [token, value] if replacement is None else replacement
        elif token == "--task-ids-file":
            notes.append("--task-ids-file FILE is now --task-ids @FILE")
            upgraded += ["--task-ids", f"@{next(tokens, (0, ''))[1]}"]
        elif token == _POSITIONAL_FLAGS.get(command):
            notes.append(f"{token} is now an argument")
            value = next(tokens, (0, ""))[1]
            trailing += value.replace(",", " ").split() if token == "--ids" else [value]
        elif token == "--retry" and command_index is not None and command in ("resume", "retry"):
            if command == "resume":
                notes.append("run resume --retry is now run retry")
            upgraded[command_index] = "retry"
        else:
            upgraded.append(token)
    if (
        command == "output"
        and command_index is not None
        and _count_arguments(upgraded[command_index + 1 :]) + len(tail[1:]) < 2
    ):
        trailing.append("")
    return upgraded + trailing + tail, notes


def _count_arguments(args: list[str]) -> int:
    count, skip = 0, False
    for token in args:
        if skip:
            skip = False
        elif token in ("-o", "--output"):
            skip = True
        elif not token.startswith("-"):
            count += 1
    return count
