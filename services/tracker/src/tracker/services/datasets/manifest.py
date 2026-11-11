from collections.abc import Mapping

from tracker.exceptions import ManifestMismatchError
from tracker.types import VersionManifest


def check_manifest(tasks: Mapping[str, str], manifest: VersionManifest) -> None:
    listed = set(manifest.tasks)
    published = set(tasks)
    if only_published := sorted(published - listed):
        raise ManifestMismatchError(f"The manifest does not list: {', '.join(only_published)}")
    if only_listed := sorted(listed - published):
        raise ManifestMismatchError(f"The manifest lists tasks that are not published: {', '.join(only_listed)}")
    for task_id in sorted(published):
        expected = manifest.tasks[task_id].digest
        if tasks[task_id] != expected:
            raise ManifestMismatchError(
                f"Task '{task_id}' is listed with digest {expected} but its files have digest {tasks[task_id]}"
            )
