import http.client
import urllib.request
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urlparse

import click

_REMOTE_TIMEOUT_SECONDS = 30
_REMOTE_MAX_BYTES = 16 * 1024 * 1024


def _clean_task_ids(task_ids: Iterable[str]) -> list[str]:
    cleaned = [task_id.strip() for task_id in task_ids]
    return list(dict.fromkeys(task_id for task_id in cleaned if task_id))


def read_task_ids_source(source: str) -> list[str]:
    try:
        if urlparse(source).scheme in {"http", "https"}:
            with urllib.request.urlopen(source, timeout=_REMOTE_TIMEOUT_SECONDS) as response:
                body = response.read(_REMOTE_MAX_BYTES + 1)
                if len(body) > _REMOTE_MAX_BYTES:
                    raise click.ClickException(f"Task ids at {source} exceed {_REMOTE_MAX_BYTES // (1024 * 1024)} MiB")
                if response.length:
                    raise click.ClickException(f"Task ids at {source} were truncated")
            content = body.decode("utf-8")
        else:
            content = Path(source).read_text(encoding="utf-8")
    except (OSError, http.client.HTTPException) as exc:
        raise click.ClickException(f"Failed to read task ids from {source}: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise click.ClickException(f"Task ids in {source} are not valid UTF-8: {exc}") from exc

    task_ids = _clean_task_ids(content.splitlines())
    if not task_ids:
        raise click.ClickException(f"No task ids found in {source}")
    return task_ids


def resolve_task_ids(task_ids: str | None = None, task_ids_file: str | None = None) -> list[str] | None:
    if task_ids is not None and task_ids_file is not None:
        raise click.UsageError("--task-ids and --task-ids-file are mutually exclusive")
    if task_ids_file is not None:
        return read_task_ids_source(task_ids_file)
    if task_ids is not None:
        resolved_task_ids = _clean_task_ids(task_ids.split(","))
        if not resolved_task_ids:
            raise click.UsageError("No task ids provided")
        return resolved_task_ids
    return None
