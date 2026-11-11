import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

SEARCH_COST_PATH_ENV = "VALKYRIE_SEARCH_COST_PATH"
SEARCH_COST_MAX_BYTES = 1024 * 1024
SEARCH_COST_MAX_USD = Decimal(100)
_PROVIDER = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}")
_SOURCE_ID = re.compile(r"[\x21-\x7e]{1,256}")


@dataclass(frozen=True)
class SearchCharge:
    provider: str
    amount_usd: Decimal
    source_id: str | None = None


def _charge(line: str) -> SearchCharge:
    row = json.loads(line, parse_float=Decimal, parse_int=Decimal)
    if not isinstance(row, dict) or row.keys() - {"provider", "amount_usd", "source_id"}:
        raise ValueError("unexpected search cost fields")
    provider, amount, source_id = row.get("provider"), row.get("amount_usd"), row.get("source_id")
    if not isinstance(provider, str) or not _PROVIDER.fullmatch(provider):
        raise ValueError("invalid provider")
    if source_id is not None and (not isinstance(source_id, str) or not _SOURCE_ID.fullmatch(source_id)):
        raise ValueError("invalid source_id")
    try:
        amount_usd = Decimal(amount) if isinstance(amount, (str, Decimal)) else None
    except InvalidOperation:
        amount_usd = None
    if amount_usd is None or not amount_usd.is_finite() or not 0 <= amount_usd <= SEARCH_COST_MAX_USD:
        raise ValueError(f"amount_usd must be a decimal from 0 to {SEARCH_COST_MAX_USD}")
    return SearchCharge(provider=provider, amount_usd=amount_usd, source_id=source_id)


def parse_search_costs(content: str) -> tuple[list[SearchCharge], int]:
    charges: list[SearchCharge] = []
    invalid = 0
    for line in content.splitlines():
        if not line.strip():
            continue
        try:
            charges.append(_charge(line))
        except (ValueError, TypeError, RecursionError):
            invalid += 1
    return charges, invalid


def search_cost_source_id(attempt_id: str, step_name: str | None) -> str:
    return attempt_id if step_name is None else f"{attempt_id}:{step_name}"


def attempt_totals(charges: list[SearchCharge], attempt_id: str) -> list[SearchCharge]:
    by_source: dict[tuple[str, str], Decimal] = {}
    for charge in charges:
        if charge.source_id is None:
            key = (charge.provider, attempt_id)
            by_source[key] = by_source.get(key, Decimal(0)) + charge.amount_usd
        else:
            by_source.setdefault((charge.provider, charge.source_id), charge.amount_usd)
    return [SearchCharge(provider, amount, source_id) for (provider, source_id), amount in by_source.items()]
