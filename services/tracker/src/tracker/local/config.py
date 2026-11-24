from pathlib import Path

import yaml

from tracker.local.resources import LocalResources


resources: LocalResources | None = None


def configure(path: Path) -> None:
    global resources
    values = yaml.safe_load(path.read_text())
    if not {"secrets_file", "secrets_manager_region"} & values.keys() and (path.parent / ".env").is_file():
        values["secrets_file"] = ".env"
    for key in ("data_root", "secrets_file"):
        if values.get(key) is not None:
            values[key] = path.parent.resolve() / values[key]
    config = LocalResources.model_validate(values)
    if config.secrets_file is not None and not config.secrets_file.is_file():
        raise ValueError(f"secrets_file must name an existing file: {config.secrets_file}")
    resources = config
