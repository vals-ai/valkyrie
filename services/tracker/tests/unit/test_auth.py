import pytest
from fastapi import HTTPException
from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.auth import forward_tracker_api_key
from tracker.config import BenchmarkServiceDestination
from tracker.database.models import DEFAULT_ORG_NAME, Org


@pytest.mark.description("Default organization resolution.")
class TestGetDefaultOrg:
    @pytest.mark.description("The default org is the Vals org.")
    def test_get_default_org_returns_vals_org(self, empty_database_session: Session) -> None:
        from tracker.auth import get_default_org

        vals_org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
        empty_database_session.add(vals_org)
        empty_database_session.commit()

        result = get_default_org(empty_database_session)
        assert result.id == TEST_ORG_ID
        assert result.name == DEFAULT_ORG_NAME

    @pytest.mark.description("A missing default org raises.")
    def test_get_default_org_raises_if_missing(self, empty_database_session: Session) -> None:
        from tracker.auth import get_default_org

        with pytest.raises(RuntimeError, match="Default org not found"):
            get_default_org(empty_database_session)

    @pytest.mark.description("A recreated default org is returned with its new ID and settings.")
    def test_get_default_org_reads_the_current_row(self, empty_database_session: Session) -> None:
        from tracker.auth import get_default_org

        empty_database_session.add(Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME))
        empty_database_session.commit()
        empty_database_session.delete(get_default_org(empty_database_session))
        empty_database_session.commit()
        recreated = Org(name=DEFAULT_ORG_NAME, requires_project=True)
        empty_database_session.add(recreated)
        empty_database_session.commit()

        result = get_default_org(empty_database_session)

        assert (result.id, result.requires_project) == (recreated.id, True)


@pytest.mark.description("Tracker API key forwarding and header validation.")
class TestForwardTrackerApiKey:
    @pytest.mark.description("The Tracker API key is forwarded to a hosted service.")
    def test_forward_tracker_api_key_injects_key_for_hosted_service(self) -> None:
        headers = forward_tracker_api_key(
            {},
            "tracker-api-key",
            destination=BenchmarkServiceDestination.HOSTED,
        )

        assert headers == {"X-Descope-Api-Key": "tracker-api-key"}

    @pytest.mark.description("An explicit key header is kept, whatever its case.")
    @pytest.mark.parametrize(
        "destination",
        [BenchmarkServiceDestination.HOSTED, BenchmarkServiceDestination.CUSTOM],
    )
    def test_forward_tracker_api_key_preserves_explicit_override_case_insensitive(
        self,
        destination: BenchmarkServiceDestination,
    ) -> None:
        original_headers = {"x-descope-api-key": "override-key"}

        headers = forward_tracker_api_key(
            original_headers,
            "tracker-api-key",
            destination=destination,
        )

        assert headers == original_headers
        assert headers is not original_headers

    @pytest.mark.description("The Tracker API key is not forwarded to a custom service.")
    def test_forward_tracker_api_key_does_not_inject_key_for_custom_service(self) -> None:
        headers = forward_tracker_api_key(
            {"Authorization": "Bearer service-key"},
            "tracker-api-key",
            destination=BenchmarkServiceDestination.CUSTOM,
        )

        assert headers == {"Authorization": "Bearer service-key"}

    @pytest.mark.description("Reject caller headers that can alter HTTP routing or protocol handling.")
    @pytest.mark.parametrize(
        "header_name",
        [
            "Host",
            "Connection",
            "Upgrade",
            "Transfer-Encoding",
            "Forwarded",
            "X-Forwarded-For",
            "Sec-WebSocket-Key",
            "X-Api-Key",
        ],
    )
    def test_forward_tracker_api_key_rejects_protocol_and_routing_headers(self, header_name: str) -> None:
        with pytest.raises(HTTPException) as exc_info:
            forward_tracker_api_key(
                {header_name: "attacker-controlled"},
                "tracker-api-key",
                destination=BenchmarkServiceDestination.HOSTED,
            )

        assert exc_info.value.status_code == 422
        assert exc_info.value.detail == "Unsupported benchmark service header"

    @pytest.mark.description("Reject headers that the outbound HTTP transport cannot encode or send.")
    @pytest.mark.parametrize(
        ("header_name", "header_value"),
        [
            (":authority", "attacker.example"),
            ("bad header", "attacker-controlled"),
            ("Authorization", "Bearer token\r\nHost: attacker.example"),
            ("Authorization", "Bearer token\x00suffix"),
            ("Authorization", "non-ascii-é"),
        ],
    )
    def test_forward_tracker_api_key_rejects_malformed_headers(self, header_name: str, header_value: str) -> None:
        with pytest.raises(HTTPException) as exc_info:
            forward_tracker_api_key(
                {header_name: header_value},
                "tracker-api-key",
                destination=BenchmarkServiceDestination.HOSTED,
            )

        assert exc_info.value.status_code == 422
        assert exc_info.value.detail == "Unsupported benchmark service header"
