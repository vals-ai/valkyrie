import json
import os
from pathlib import Path
from typing import Any

import click
from click.shell_completion import CompletionItem, ShellComplete, add_completion_class, get_completion_class
from click.shell_completion import FishComplete as ClickFishComplete

COMPLETION_FILE = Path(__file__).with_name("completion.json")

_NU_SOURCE = """# Install: valkyrie completion nu | save --force ~/.config/nushell/valkyrie.nu
# Add source ~/.config/nushell/valkyrie.nu to config.nu.
let valkyrie_completer = {|spans|
    let executable = ($spans | first)
    with-env {
        _VALKYRIE_COMPLETE: nu_complete
        _VALK_COMPLETE: nu_complete
        _VALKYRIE_COMPLETE_ARGS: ($spans | skip 1 | to json --raw)
    } {
        let completions = (^$executable | lines | each --keep-empty {|line| $line | from json })
        if ($completions | any {|item| $item == null }) { null } else { $completions }
    }
}
let previous_completer = ($env.config.completions.external.completer? | default null)
$env.config.completions.external = ($env.config.completions.external | merge {
    enable: true
    completer: {|spans|
        if ($spans | first | path basename) in [valkyrie valk] {
            do $valkyrie_completer $spans
        } else if $previous_completer != null {
            do $previous_completer $spans
        } else {
            null
        }
    }
})
"""


@add_completion_class
class NuComplete(ShellComplete):
    name = "nu"
    source_template = _NU_SOURCE

    def get_completion_args(self) -> tuple[list[str], str]:
        words: list[str] = json.loads(os.environ["_VALKYRIE_COMPLETE_ARGS"])
        return words[:-1], words[-1] if words else ""

    def format_completion(self, item: CompletionItem) -> str:
        if item.type in {"file", "dir"}:
            return "null"
        return json.dumps({"value": item.value, "description": item.help or ""})


@add_completion_class
class FishComplete(ClickFishComplete):
    def source(self) -> str:
        return super().source().replace('string split ","', 'string split --max 1 ","')


def completion_tree() -> click.Group:
    records: list[dict[str, Any]] = json.loads(COMPLETION_FILE.read_text())
    commands: dict[tuple[str, ...], click.Command] = {}
    for record in records:
        path = tuple(record["path"])
        params: list[click.Parameter] = []
        for saved in record["params"]:
            parameter = dict(saved)
            names = parameter.pop("names")
            kind = parameter.pop("kind")
            choices = parameter.pop("choices", None)
            case_sensitive = parameter.pop("case_sensitive", True)
            path_type = parameter.pop("path", None)
            parameter["type"] = (
                click.Choice(choices, case_sensitive=case_sensitive)
                if choices is not None
                else click.Path(**path_type)
                if path_type is not None
                else click.STRING
            )
            params.append((click.Option if kind == "option" else click.Argument)(names, **parameter))
        command_class = click.Group if record["group"] else click.Command
        command = command_class(
            path[-1] if path else "valkyrie",
            params=params,
            short_help=record["help"],
            add_help_option=False,
        )
        commands[path] = command
        if path:
            parent = commands[path[:-1]]
            assert isinstance(parent, click.Group)
            parent.add_command(command)
    root = commands[()]
    assert isinstance(root, click.Group)
    return root


@click.command(
    help=(
        "Print completion scripts for both valkyrie and valk.\n\n"
        'Bash: add eval "$(valkyrie completion bash)" to ~/.bashrc.\n\n'
        'Zsh: add eval "$(valkyrie completion zsh)" after compinit in ~/.zshrc.\n\n'
        "Fish: add valkyrie completion fish | source to ~/.config/fish/config.fish.\n\n"
        "Nushell: save valkyrie completion nu to ~/.config/nushell/valkyrie.nu, "
        "then source that file from config.nu."
    ),
    short_help="Print a shell completion script.",
)
@click.argument("shell", type=click.Choice(["bash", "zsh", "fish", "nu"]))
@click.pass_context
def completion(ctx: click.Context, shell: str) -> None:
    root = ctx.find_root()
    completion_class = get_completion_class(shell)
    assert completion_class is not None
    if shell == "nu":
        click.echo(completion_class(root.command, {}, "valkyrie", "_VALKYRIE_COMPLETE").source())
        return
    installation = {
        "bash": 'eval "$(valkyrie completion bash)" in ~/.bashrc.',
        "zsh": 'eval "$(valkyrie completion zsh)" after compinit in ~/.zshrc.',
        "fish": "valkyrie completion fish | source in ~/.config/fish/config.fish.",
    }
    click.echo(f"# Install: {installation[shell]}")
    for prog_name in ("valkyrie", "valk"):
        complete_var = f"_{prog_name.upper()}_COMPLETE"
        click.echo(completion_class(root.command, {}, prog_name, complete_var).source())
