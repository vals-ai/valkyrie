"""Names for GPU types, and the way each provider's own names are found among them."""

import re
from collections.abc import Mapping, Sequence
from enum import Enum

from daytona.v0 import GpuType

from benchmark_service.sandbox.types import SandboxError


class GpuModel(Enum):
    """A GPU type: its key and other spellings of it."""

    T4 = ("t4", ())
    L4 = ("l4", ())
    A10 = ("a10", ())
    L40S = ("l40s", ())
    A100 = ("a100", ())
    A100_40GB = ("a100-40gb", ())
    A100_80GB = ("a100-80gb", ())
    H100 = ("h100", ("h100-80gb",))
    H100_ONLY = ("h100!", ())
    H200 = ("h200", ("h200-141gb",))
    B200 = ("b200", ())
    B200_PLUS = ("b200+", ())
    B300 = ("b300", ())
    RTX_PRO_6000 = ("rtx-pro-6000", ("rtx-pro6000", "rtx-6000-pro", "rtx-pro-6000-blackwell"))
    RTX_4090 = ("rtx-4090", ("rtx4090", "4090"))
    RTX_5090 = ("rtx-5090", ("rtx5090", "5090"))
    MI355X = ("mi355x", ())

    def __init__(self, key: str, aliases: tuple[str, ...]) -> None:
        self.key: str = key
        self.aliases: tuple[str, ...] = aliases


_BY_KEY = {key: model for model in GpuModel for key in (model.key, *model.aliases)}


def normalize_gpu_name(name: str) -> str:
    key = re.sub(r"^(nvidia-)?(geforce-|tesla-)?", "", re.sub(r"[\s_-]+", "-", name.strip().lower()))
    model = _BY_KEY.get(key)
    return model.key if model else key


# Modal's SDK takes the GPU as a string and ships no list of them: https://modal.com/docs/guide/gpu
MODAL_GPU_TYPES = (
    "T4",
    "L4",
    "A10",
    "L40S",
    "A100",
    "A100-40GB",
    "A100-80GB",
    "H100",
    "H100!",
    "H200",
    "B200",
    "B200+",
    "B300",
    "RTX-PRO-6000",
)
_DAYTONA_GPU_TYPES = {
    normalize_gpu_name(gpu.value): gpu.value for gpu in GpuType if gpu is not GpuType.UNKNOWN_DEFAULT_OPEN_API
}
_MODAL_GPU_TYPES = {normalize_gpu_name(gpu): gpu for gpu in MODAL_GPU_TYPES}


def gpu_models(names: Sequence[str]) -> list[GpuModel]:
    """The GPU types the names give, in order, ignoring blanks. Unknown names are refused."""
    keys = {name: key for name in names if (key := normalize_gpu_name(name))}
    if unknown := [name for name, key in keys.items() if key not in _BY_KEY]:
        known = ", ".join(model.key for model in GpuModel)
        raise SandboxError(f"Unknown GPU type: {', '.join(unknown)}. Known types: {known}")
    return list(dict.fromkeys(_BY_KEY[key] for key in keys.values()))


def _offered(provider: str, offered: Mapping[str, str], names: Sequence[str]) -> list[str]:
    """The provider's types for the names, in order. Names it offers none of are refused."""
    models = gpu_models(names)
    found = [offered[model.key] for model in models if model.key in offered]
    if models and not found:
        supported = ", ".join(offered.values())
        raise SandboxError(f"Unsupported {provider} GPU type: {', '.join(names)}. Supported types: {supported}")
    return found


def daytona_gpu_type(names: Sequence[str]) -> GpuType | list[GpuType] | None:
    found = [GpuType(gpu) for gpu in _offered("Daytona", _DAYTONA_GPU_TYPES, names)]
    return found if len(found) > 1 else next(iter(found), None)


def modal_gpu_type(names: Sequence[str]) -> str | None:
    return next(iter(_offered("Modal", _MODAL_GPU_TYPES, names)), None)
