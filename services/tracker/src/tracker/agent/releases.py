import hashlib
import io
import json
import re
import stat
import zipfile
from pathlib import PurePosixPath

from tracker.agent.contract import get_contract_from_zip_bytes
from tracker.agent.schemas import AgentConfig, validate_agent_name
from tracker.exceptions import BundlerError
from tracker.types import AgentContractRequest, AgentReleaseSelection
from tracker.runtime.storage import ObjectStore


def retained_agent_key(name: str, selection: AgentReleaseSelection) -> str:
    validate_agent_name(name)
    if selection.archive_sha256 is None:
        raise ValueError("Agent release must be resolved before reading its archive")
    return f"agent-releases/{name}/{selection.reference}/{selection.archive_sha256}.zip"


def validate_archive(content: bytes, selection: AgentReleaseSelection) -> None:
    if hashlib.sha256(content).hexdigest() != selection.archive_sha256:
        raise ValueError("Agent release archive digest mismatch")
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        seen: set[str] = set()
        for entry in archive.infolist():
            path = PurePosixPath(entry.filename)
            if path.is_absolute() or ".." in path.parts or "\\" in entry.filename:
                raise ValueError("Unsafe agent archive path")
            if entry.filename in seen or stat.S_ISLNK(entry.external_attr >> 16):
                raise ValueError("Duplicate path or symbolic link in agent archive")
            seen.add(entry.filename)
        if archive.testzip() is not None:
            raise ValueError("Corrupt agent release archive")


async def resolve_agent_release(store: ObjectStore, contract: AgentContractRequest) -> AgentContractRequest:
    selection = contract.agent_release
    assert selection is not None
    configuration = {**selection.prescribed_configuration, **selection.overrides}
    if "model" in configuration:
        raise ValueError("Pass the evaluated model as the model input")
    if any(
        re.search(r"(^|_)(secret|password|credential|api_key|access_token|private_key)($|_)", key, re.I)
        for key in configuration
    ):
        raise ValueError("Agent configuration cannot contain credentials; use named secret references")
    validate_agent_name(contract.name)
    receipt = json.loads(await store.get_bytes(f"agent-releases/{contract.name}/{selection.reference}/release.json"))
    if (
        not isinstance(receipt, dict)
        or receipt.get("schema_version") != 1
        or receipt.get("agent") != contract.name
        or receipt.get("archive") != f"{contract.name}.zip"
        or not isinstance(receipt.get("sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", receipt["sha256"]) is None
        or (selection.archive_sha256 is not None and receipt["sha256"] != selection.archive_sha256)
    ):
        raise ValueError("Agent release receipt does not match the selected archive")
    selection = AgentReleaseSelection.model_validate({**selection.model_dump(), "archive_sha256": receipt["sha256"]})
    content = await store.get_bytes(retained_agent_key(contract.name, selection))
    validate_archive(content, selection)
    try:
        resolved = get_contract_from_zip_bytes(
            contract.name,
            content,
            AgentConfig(model=contract.model, kwargs=configuration),
        )
    except BundlerError as exc:
        raise ValueError(str(exc)) from exc
    if contract.kwargs:
        raise ValueError("Exact releases require configuration in prescribed_configuration and overrides")
    resolved.model = contract.model or resolved.kwargs.get("model")
    resolved.agent_release = selection
    resolved.secrets = {**resolved.secrets, **contract.secrets}
    resolved.inference_settings_attested = True
    return resolved


async def preserve_attempt_outputs(store: ObjectStore, run_id: str, recovery_id: str) -> str:
    root = f"benchmarks/{run_id}/"
    destination = f"{root}agent-attempts/{recovery_id}/"
    keys = [item.key async for item in store.list_objects(root) if not item.key.startswith(f"{root}agent-attempts/")]
    for key in keys:
        await store.copy(key, destination + key.removeprefix(root))
    return destination
