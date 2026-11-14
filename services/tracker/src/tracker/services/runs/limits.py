import logging
import re
from collections.abc import Callable
from threading import Event, Thread
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel, ConfigDict, NonNegativeInt, ValidationError
from sqlmodel import Session

from tracker import config
from tracker.clients.parameters import read_parameter
from tracker.exceptions import RunLimitExceededError
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.organization import OrganizationRepository

logger = logging.getLogger(__name__)

RUN_LIMITS_REFRESH_SECONDS = 60
RUN_LIMITS_SHUTDOWN_TIMEOUT_SECONDS = 5
RUN_LIMIT_RETRY_AFTER_SECONDS = 60


class OrgRunLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    default: NonNegativeInt | None = None
    exempt: frozenset[str] = frozenset()
    exempt_patterns: tuple[re.Pattern[str], ...] = ()
    overrides: dict[str, NonNegativeInt] = {}

    def for_org(self, org_name: str) -> int | None:
        if org_name in self.exempt or any(pattern.fullmatch(org_name) for pattern in self.exempt_patterns):
            return None
        return self.overrides.get(org_name, self.default)


def read_run_limits_parameter() -> str | None:
    if config.ORG_RUN_LIMITS_PARAMETER is None:
        return None
    return read_parameter(config.ORG_RUN_LIMITS_PARAMETER)


class OrgRunLimitsCache:
    def __init__(self, read: Callable[[], str | None] = read_run_limits_parameter) -> None:
        self._read = read
        self._limits = OrgRunLimits()

    def current(self) -> OrgRunLimits:
        return self._limits

    def refresh(self) -> None:
        try:
            value = self._read()
            self._limits = OrgRunLimits() if value is None else OrgRunLimits.model_validate_json(value)
        except (BotoCoreError, ClientError, ValidationError):
            logger.exception("Failed to load organization run limits; keeping the previous limits")


run_limits = OrgRunLimitsCache()


def run_limits_refresh_loop(stop_event: Event, *, interval_seconds: float = RUN_LIMITS_REFRESH_SECONDS) -> None:
    while not stop_event.wait(interval_seconds):
        run_limits.refresh()


class AutomaticRunLimitsRefresh:
    def __init__(self, *, interval_seconds: float = RUN_LIMITS_REFRESH_SECONDS) -> None:
        self._stop_event = Event()
        self._thread = Thread(
            target=run_limits_refresh_loop,
            args=(self._stop_event,),
            kwargs={"interval_seconds": interval_seconds},
            name="run-limits-refresh",
            daemon=True,
        )

    def start(self) -> None:
        run_limits.refresh()
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(RUN_LIMITS_SHUTDOWN_TIMEOUT_SECONDS)
        if self._thread.is_alive():
            logger.error("Run limits refresh thread did not stop before shutdown")


def ensure_run_capacity(session: Session, org_id: UUID, run_id: UUID) -> None:
    org = OrganizationRepository(session).get(org_id)
    assert org is not None
    limit = run_limits.current().for_org(org.name)
    if limit is None:
        return
    OrganizationRepository(session).lock_run_capacity(org_id)
    active = BenchmarkRepository(session).count_active(org_id, excluding=run_id)
    if active >= limit:
        raise RunLimitExceededError(limit=limit, active=active)
