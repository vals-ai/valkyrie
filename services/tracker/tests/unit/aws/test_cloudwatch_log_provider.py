from __future__ import annotations

from collections import deque
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from uuid import uuid4

import pytest  # pyright: ignore[reportMissingImports]
from botocore.exceptions import ClientError, EndpointConnectionError  # pyright: ignore[reportMissingImports]

from tracker.aws import cloudwatch_logs
from tracker.aws.clients import AWSClientProvider
from tracker.aws.cloudwatch_logs import CloudWatchLogProvider, task_log_stream_name
from tracker.runtime.logs import (
    LogEvent,
    LogProviderError,
    RunLogReference,
    RunTaskLogReference,
    TaskLogLine,
    TaskLogReference,
)

_legacy_task_log_stream_name = getattr(cloudwatch_logs, "_legacy_task_log_stream_name")


class MockLogsClient:
    def __init__(self, responses: list[dict[str, Any] | BaseException]) -> None:
        self.responses = deque(responses)
        self.filter_requests: list[dict[str, Any]] = []
        self.get_requests: list[dict[str, Any]] = []

    def filter_log_events(self, **request: Any) -> dict[str, Any]:
        self.filter_requests.append(request)
        return self._response()

    def get_log_events(self, **request: Any) -> dict[str, Any]:
        self.get_requests.append(request)
        return self._response()

    def _response(self) -> dict[str, Any]:
        response = self.responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response


class MockClients:
    def __init__(self, logs_client: MockLogsClient) -> None:
        self.logs_client = logs_client

    def cloudwatch_logs_client(self) -> MockLogsClient:
        return self.logs_client


class FailingClients:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def cloudwatch_logs_client(self) -> MockLogsClient:
        raise self.error


def _provider(logs_client: MockLogsClient) -> CloudWatchLogProvider:
    return CloudWatchLogProvider(cast(AWSClientProvider, MockClients(logs_client)), "benchmarks")


@pytest.mark.description("Snapshot reads must translate client construction failures and retain their cause.")
@pytest.mark.parametrize(
    ("failure", "message"),
    [
        (
            ClientError({"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "FilterLogEvents"),
            "AccessDeniedException",
        ),
        (EndpointConnectionError(endpoint_url="https://logs.example.com"), "CloudWatch log request failed"),
    ],
)
async def test_snapshot_translates_client_construction_errors(failure: BaseException, message: str) -> None:
    provider = CloudWatchLogProvider(cast(AWSClientProvider, FailingClients(failure)), "benchmarks")

    with pytest.raises(LogProviderError, match=message) as error:
        await provider.fetch(RunLogReference(run_id=uuid4()))

    assert error.value.__cause__ is failure


@pytest.mark.description(
    "Run reads must use exact stream identities, deterministic order, and unchanged end milliseconds."
)
async def test_run_fetch_uses_exact_stream_metadata_and_inclusive_millisecond_bound() -> None:
    run_id = uuid4()
    first_started = datetime(2026, 1, 1, 11, 59, tzinfo=timezone.utc)
    second_started = datetime(2026, 1, 1, 11, 58, tzinfo=timezone.utc)
    start = datetime(2026, 1, 1, 12, 0, 0, 123_999, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 12, 5, 0, 456_789, tzinfo=timezone.utc)
    logs_client = MockLogsClient(
        [
            {
                "events": [
                    {
                        "timestamp": 2_000,
                        "ingestionTime": 2_100,
                        "message": '{"message": "second", "trace_id": "abc"}',
                        "logStreamName": task_log_stream_name("task_two", second_started),
                        "eventId": "event-2",
                    },
                    {
                        "timestamp": 1_000,
                        "ingestionTime": 1_100,
                        "message": "first",
                        "logStreamName": task_log_stream_name("task_one", first_started),
                        "eventId": "event-1",
                    },
                ],
                "nextToken": "next-page",
            }
        ]
    )

    page = await _provider(logs_client).fetch(
        RunLogReference(
            run_id=run_id,
            tasks=(
                RunTaskLogReference(task_id="task_one", started_at=first_started),
                RunTaskLogReference(task_id="task_two", started_at=second_started),
            ),
        ),
        start_time=start,
        end_time=end,
        cursor="current-page",
        limit=50,
    )

    assert logs_client.filter_requests[0] == {
        "logGroupName": f"benchmarks/{run_id}",
        "limit": 50,
        "startTime": int(start.timestamp() * 1_000),
        "endTime": int(end.timestamp() * 1_000),
        "nextToken": "current-page",
    }
    assert [event.message for event in page.events] == ["first", "second"]
    assert [event.task_id for event in page.events] == ["task_one", "task_two"]
    assert page.next_cursor == "next-page"


@pytest.mark.description("Queries must be applied locally so CloudWatch cannot interpret filter metacharacters.")
async def test_run_fetch_filters_cloudwatch_metacharacters_as_literal_substrings() -> None:
    run_id = uuid4()
    query = '* "quoted" \\'
    logs_client = MockLogsClient(
        [
            {
                "events": [
                    {"timestamp": 1_000, "message": f"prefix {query} suffix"},
                    {"timestamp": 2_000, "message": "prefix wildcard-like text suffix"},
                ],
                "nextToken": "next-page",
            }
        ]
    )

    page = await _provider(logs_client).fetch(RunLogReference(run_id=run_id), query=query)

    assert [event.message for event in page.events] == [f"prefix {query} suffix"]
    assert "filterPattern" not in logs_client.filter_requests[0]
    assert page.next_cursor == "next-page"


@pytest.mark.description("Colliding legacy names must remain aggregate-only while canonical names stay distinct.")
async def test_collision_resistant_names_do_not_attribute_ambiguous_legacy_stream() -> None:
    run_id = uuid4()
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first = RunTaskLogReference(task_id="task:one", started_at=started_at)
    second = RunTaskLogReference(task_id="task*one", started_at=started_at)
    legacy_name = _legacy_task_log_stream_name(first.task_id, first.started_at)
    assert legacy_name == _legacy_task_log_stream_name(second.task_id, second.started_at)
    assert task_log_stream_name(first.task_id, first.started_at) != task_log_stream_name(
        second.task_id, second.started_at
    )
    logs_client = MockLogsClient(
        [{"events": [{"timestamp": 1_000, "message": "ambiguous", "logStreamName": legacy_name}]}]
    )

    page = await _provider(logs_client).fetch(RunLogReference(run_id=run_id, tasks=(first, second)))

    assert page.events[0].task_id is None


@pytest.mark.description("A selected task must not read a canonical name used by a sibling's legacy stream.")
async def test_task_fetch_excludes_canonical_name_shared_with_sibling_legacy_stream() -> None:
    run_id = uuid4()
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    task_id = "task:one"
    canonical_name = task_log_stream_name(task_id, started_at)
    sibling = RunTaskLogReference(task_id=canonical_name.rsplit("_", 1)[0], started_at=started_at)
    assert _legacy_task_log_stream_name(sibling.task_id, sibling.started_at) == canonical_name
    reference = TaskLogReference(
        run_id=run_id,
        task_id=task_id,
        started_at=started_at,
        siblings=(sibling,),
    )
    logs_client = MockLogsClient([{"events": []}])

    await _provider(logs_client).fetch(reference)

    assert logs_client.filter_requests[0]["logStreamNames"] == [
        _legacy_task_log_stream_name(reference.task_id, started_at)
    ]


@pytest.mark.description("A task read must fail instead of selecting a stream shared with a sibling.")
async def test_task_fetch_rejects_when_all_names_are_ambiguous() -> None:
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    reference = TaskLogReference(
        run_id=uuid4(),
        task_id="task_one",
        started_at=started_at,
        siblings=(RunTaskLogReference(task_id="task:one", started_at=started_at),),
    )
    logs_client = MockLogsClient([{"events": []}])

    with pytest.raises(LogProviderError, match="ambiguous"):
        await _provider(logs_client).fetch(reference)

    assert logs_client.filter_requests == []


@pytest.mark.description("A selected task must not read a legacy stream shared by a sibling task.")
async def test_task_fetch_excludes_ambiguous_legacy_stream() -> None:
    run_id = uuid4()
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    sibling = RunTaskLogReference(task_id="task*one", started_at=started_at)
    reference = TaskLogReference(
        run_id=run_id,
        task_id="task:one",
        started_at=started_at,
        siblings=(sibling,),
    )
    logs_client = MockLogsClient([{"events": []}])

    await _provider(logs_client).fetch(reference)

    assert logs_client.filter_requests[0]["logStreamNames"] == [task_log_stream_name(reference.task_id, started_at)]


@pytest.mark.description(
    "A page with logs behind empty CloudWatch pages returns those logs instead of an empty page with a cursor."
)
async def test_run_fetch_skips_empty_pages_to_the_next_events() -> None:
    reference = RunLogReference(run_id=uuid4(), tasks=())
    logs_client = MockLogsClient(
        [
            {"events": [], "nextToken": "page-2"},
            {"events": [], "nextToken": "page-3"},
            {"events": [{"timestamp": 1_000, "message": "agent output"}], "nextToken": "page-4"},
        ]
    )

    page = await _provider(logs_client).fetch(reference)

    assert [event.message for event in page.events] == ["agent output"]
    assert page.next_cursor == "page-4"
    assert [request.get("nextToken") for request in logs_client.filter_requests] == [None, "page-2", "page-3"]


@pytest.mark.description("Empty CloudWatch pages are followed until the token stops advancing.")
async def test_task_fetch_follows_empty_pages_until_the_cursor_is_stable() -> None:
    run_id = uuid4()
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    reference = TaskLogReference(
        run_id=run_id,
        task_id="provider/model:fast",
        started_at=started_at,
        siblings=(),
    )
    logs_client = MockLogsClient(
        [
            {"events": [], "nextToken": "advanced"},
            {"events": [], "nextToken": "advanced"},
        ]
    )
    provider = _provider(logs_client)

    page = await provider.fetch(reference)

    assert page.events == []
    assert page.next_cursor is None
    assert [request.get("nextToken") for request in logs_client.filter_requests] == [None, "advanced"]
    assert logs_client.filter_requests[0]["logStreamNames"] == [
        task_log_stream_name(reference.task_id, started_at),
        _legacy_task_log_stream_name(reference.task_id, started_at),
    ]


@pytest.mark.description("A task log fetch reads whichever stream name exists.")
@pytest.mark.parametrize("available_stream", ["canonical", "legacy", None])
async def test_task_fetch_reads_existing_stream_when_other_name_is_absent(available_stream: str | None) -> None:
    reference = TaskLogReference(
        run_id=uuid4(), task_id="task:one", started_at=datetime(2026, 1, 1, tzinfo=timezone.utc), siblings=()
    )
    missing = ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "FilterLogEvents")
    found = {"events": [{"timestamp": 1_000, "message": "agent output"}], "nextToken": "next-page"}
    responses: list[dict[str, Any] | BaseException] = [missing]
    if available_stream != "canonical":
        responses.append(missing)
    responses.append(found if available_stream is not None else missing)
    logs_client = MockLogsClient(responses)

    page = await _provider(logs_client).fetch(reference, cursor="current-page", limit=20)

    assert [event.message for event in page.events] == ([] if available_stream is None else ["agent output"])
    assert page.next_cursor == (None if available_stream is None else "next-page")
    assert all(
        request["nextToken"] == "current-page" and request["limit"] == 20 for request in logs_client.filter_requests
    )
    assert logs_client.filter_requests[1]["logStreamNames"] == [
        task_log_stream_name(reference.task_id, reference.started_at)
    ]
    if available_stream != "canonical":
        assert logs_client.filter_requests[2]["logStreamNames"] == [
            _legacy_task_log_stream_name(reference.task_id, reference.started_at)
        ]


@pytest.mark.description("Follow mode must emit a repeated event once and retain AWS failures as provider causes.")
async def test_follow_deduplicates_poll_results_and_translates_aws_errors() -> None:
    run_id = uuid4()
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    reference = TaskLogReference(run_id=run_id, task_id="task", started_at=started_at)
    repeated_event = {"timestamp": 1_000, "ingestionTime": 1_100, "message": "hello"}
    ignored_event = {"timestamp": 1_001, "ingestionTime": 1_101, "message": "noise"}
    logs_client = MockLogsClient(
        [
            {"events": [repeated_event, ignored_event], "nextForwardToken": "tail"},
            {"events": [repeated_event, ignored_event], "nextForwardToken": "tail"},
        ]
    )

    events = [
        event
        async for event in _provider(logs_client).stream_task(
            reference,
            query="hell",
            end_time=datetime(2020, 1, 1, tzinfo=timezone.utc),
            poll_interval=0,
        )
    ]

    assert [event.message for event in events] == ["hello"]
    assert (
        logs_client.get_requests[0]["endTime"] == int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000) + 1
    )

    denied = ClientError({"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "FilterLogEvents")
    failing_client = MockLogsClient([denied])
    with pytest.raises(LogProviderError, match="AccessDeniedException") as error:
        await _provider(failing_client).fetch(RunLogReference(run_id=run_id))
    assert error.value.__cause__ is denied


@pytest.mark.description("Follow mode keeps CloudWatch order for lines written in one chunk with the same timestamp.")
async def test_follow_keeps_write_order_for_same_timestamp_lines() -> None:
    reference = TaskLogReference(run_id=uuid4(), task_id="task", started_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    messages = [f"step {number}" for number in range(1, 11)]
    lines = [TaskLogLine(received_at=1.0, message=message, fields={"trace_id": "trace"}) for message in messages]
    [batch] = cloudwatch_logs._log_event_batches(lines)  # pyright: ignore[reportPrivateUsage]
    page = {"events": [{**event, "ingestionTime": 2_000} for event in batch], "nextForwardToken": "tail"}
    logs_client = MockLogsClient([page, page])

    events = [
        event
        async for event in _provider(logs_client).stream_task(
            reference, end_time=datetime(2020, 1, 1, tzinfo=timezone.utc), poll_interval=0
        )
    ]

    assert [event.message for event in events] == messages


@pytest.mark.description("Follow mode may select a unique legacy stream only after the canonical stream is missing.")
async def test_follow_falls_back_to_unique_legacy_stream_when_canonical_is_absent() -> None:
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    reference = TaskLogReference(
        run_id=uuid4(),
        task_id="task:one",
        started_at=started_at,
        siblings=(),
    )
    missing = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "missing"}},
        "GetLogEvents",
    )
    logs_client = MockLogsClient(
        [
            missing,
            {
                "events": [{"timestamp": 1_000, "message": "legacy", "eventId": "legacy"}],
                "nextForwardToken": "tail",
            },
            {"events": [], "nextForwardToken": "tail"},
        ]
    )

    events = [
        event
        async for event in _provider(logs_client).stream_task(
            reference,
            end_time=datetime(2020, 1, 1, tzinfo=timezone.utc),
            poll_interval=0,
        )
    ]

    assert [event.message for event in events] == ["legacy"]
    assert [request["logStreamName"] for request in logs_client.get_requests] == [
        task_log_stream_name(reference.task_id, started_at),
        _legacy_task_log_stream_name(reference.task_id, started_at),
        _legacy_task_log_stream_name(reference.task_id, started_at),
    ]


@pytest.mark.description(
    (
        "Following before the first write must discover the new canonical stream.\n"
        "\n"
        "Test cases:\n"
        "- Both canonical and legacy streams are initially absent.\n"
        "- The next poll reads the canonical stream without a stale cursor."
    )
)
async def test_follow_rechecks_canonical_stream_when_both_names_are_missing() -> None:
    reference = TaskLogReference(
        run_id=uuid4(),
        task_id="task:one",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        siblings=(),
    )
    missing = ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "GetLogEvents")
    logs_client = MockLogsClient([missing, missing, {"events": [{"timestamp": 1_000, "message": "first write"}]}])
    stream = cast(AsyncGenerator[LogEvent, None], _provider(logs_client).stream_task(reference, poll_interval=0))

    try:
        event = await anext(stream)
    finally:
        await stream.aclose()

    assert event.message == "first write"
    assert logs_client.get_requests[-1]["logStreamName"] == task_log_stream_name(
        reference.task_id, reference.started_at
    )
    assert "nextToken" not in logs_client.get_requests[-1]


@pytest.mark.description("A missing canonical stream must not select a legacy stream shared by a sibling.")
async def test_follow_does_not_fall_back_to_ambiguous_legacy_stream() -> None:
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    reference = TaskLogReference(
        run_id=uuid4(),
        task_id="task:one",
        started_at=started_at,
        siblings=(RunTaskLogReference(task_id="task*one", started_at=started_at),),
    )
    missing = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "missing"}},
        "GetLogEvents",
    )
    logs_client = MockLogsClient([missing])

    events = [
        event
        async for event in _provider(logs_client).stream_task(
            reference,
            end_time=datetime(2020, 1, 1, tzinfo=timezone.utc),
            poll_interval=0,
        )
    ]

    assert events == []
    assert [request["logStreamName"] for request in logs_client.get_requests] == [
        task_log_stream_name(reference.task_id, started_at)
    ]


@pytest.mark.description("Follow mode must suppress recent duplicates without retaining every emitted identity.")
async def test_follow_deduplication_retains_only_a_recent_identity_window(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tracker.aws.cloudwatch_logs._FOLLOW_DEDUPLICATION_WINDOW", 2)
    reference = TaskLogReference(
        run_id=uuid4(),
        task_id="task",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    events = [
        {"timestamp": 1_000, "message": "a", "eventId": "a"},
        {"timestamp": 2_000, "message": "b", "eventId": "b"},
        {"timestamp": 1_000, "message": "a", "eventId": "a"},
        {"timestamp": 3_000, "message": "c", "eventId": "c"},
        {"timestamp": 1_000, "message": "a", "eventId": "a"},
    ]
    responses: list[dict[str, Any] | BaseException] = [
        {"events": [event], "nextForwardToken": f"page-{index}"} for index, event in enumerate(events, start=1)
    ]
    responses.append({"events": [], "nextForwardToken": "page-5"})

    messages = [
        event.message
        async for event in _provider(MockLogsClient(responses)).stream_task(
            reference,
            end_time=datetime(2020, 1, 1, tzinfo=timezone.utc),
            poll_interval=0,
        )
    ]

    assert messages == ["a", "b", "c", "a"]


@pytest.mark.description("Persisted naive timestamps must name the same stream as their UTC instant.")
def test_task_stream_name_treats_naive_started_at_as_utc() -> None:
    naive = datetime(2026, 1, 1, 12, 30, 45, 123_456)
    utc = naive.replace(tzinfo=timezone.utc)
    offset = utc.astimezone(timezone(timedelta(hours=-7)))

    assert task_log_stream_name("task", naive) == task_log_stream_name("task", utc)
    assert task_log_stream_name("task", offset) == task_log_stream_name("task", utc)


@pytest.mark.description("A strict attempt reads only the canonical stream and marks a missing one unavailable.")
async def test_strict_attempt_uses_only_canonical_stream_and_marks_missing_unavailable() -> None:
    started = datetime.now(timezone.utc)
    reference = TaskLogReference(uuid4(), "task:legacy", started, siblings=(), strict_attempt=True)
    missing = ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "FilterLogEvents")
    client = MockLogsClient([missing, {"events": [{"timestamp": 1, "message": "legacy"}]}])
    page = await _provider(client).fetch(reference)
    assert page.available is False
    assert not page.events
    assert len(client.filter_requests) == 1
    assert client.filter_requests[0]["logStreamNames"] == [task_log_stream_name(reference.task_id, started)]
    stream_client = MockLogsClient([missing])
    with pytest.raises(LogProviderError, match="unavailable"):
        await anext(_provider(stream_client).stream_task(reference))
    assert len(stream_client.get_requests) == 1


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ('{"message": "step 1", "trace_id": "abc", "sandbox_id": "sb-1"}', "step 1"),
        ('{"message": "connected", "level": "warning"}', '{"message": "connected", "level": "warning"}'),
        ('{"msg": "connected"}', '{"msg": "connected"}'),
        ("plain line", "plain line"),
    ],
)
@pytest.mark.description(
    "Only Tracker's own JSON task log bodies are unwrapped, so older lines that happen to be JSON keep their fields."
)
def test_event_message_unwraps_only_tracker_bodies(raw: str, expected: str) -> None:
    assert cloudwatch_logs._event_message(raw) == expected  # pyright: ignore[reportPrivateUsage]


@pytest.mark.description(
    "Following empty pages stops at the request cap and returns the last token without an extra call."
)
async def test_fetch_stops_following_empty_pages_at_the_request_cap() -> None:
    logs_client = MockLogsClient([{"events": [], "nextToken": f"page-{number}"} for number in range(2, 14)])

    page = await _provider(logs_client).fetch(RunLogReference(run_id=uuid4()))

    assert page.events == []
    assert page.next_cursor == "page-11"
    assert len(logs_client.filter_requests) == 10


@pytest.mark.description("Pages followed after the single-stream fallback stay on the stream that was found.")
async def test_task_fetch_follows_empty_pages_on_the_fallback_stream() -> None:
    reference = TaskLogReference(
        run_id=uuid4(), task_id="task:one", started_at=datetime(2026, 1, 1, tzinfo=timezone.utc), siblings=()
    )
    missing = ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "FilterLogEvents")
    logs_client = MockLogsClient(
        [
            missing,
            missing,
            {"events": [], "nextToken": "page-2"},
            {"events": [{"timestamp": 1_000, "message": "agent output"}]},
        ]
    )

    page = await _provider(logs_client).fetch(reference)

    legacy = [_legacy_task_log_stream_name(reference.task_id, reference.started_at)]
    assert [event.message for event in page.events] == ["agent output"]
    assert logs_client.filter_requests[2]["logStreamNames"] == legacy
    assert logs_client.filter_requests[3]["logStreamNames"] == legacy
    assert logs_client.filter_requests[3]["nextToken"] == "page-2"
