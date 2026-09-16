import asyncio
import json
import os
import sys
from unittest.mock import AsyncMock, MagicMock
import httpx
import pytest

_sdk_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _sdk_path not in sys.path:
    sys.path.insert(0, _sdk_path)

from daytona_sdk.services.workspace_service import WorkspaceService
from daytona_sdk.utils.retry import (
    DEFAULT_RETRYABLE_EXCEPTIONS,
    async_retry_stream_with_backoff,
)


@pytest.mark.asyncio
async def test_stream_retry_on_timeout():
    """Verify stream recovers from httpx.ReadTimeout and delivers all events."""
    call_count = 0

    @async_retry_stream_with_backoff(max_retries=3, base_delay=0.01, max_delay=0.1)
    async def mock_event_stream(workspace_id: str, after: str = None):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            yield {"id": "1", "event": "provisioning_started"}
            raise httpx.ReadTimeout("Simulated connection timeout")
        yield {"id": "2", "event": "provisioning_done"}

    events = []
    async for ev in mock_event_stream("ws-123"):
        events.append(ev)

    assert len(events) == 2
    assert events[0]["id"] == "1"
    assert events[1]["id"] == "2"
    assert call_count == 2


@pytest.mark.asyncio
async def test_stream_retry_with_cursor_tracking():
    """Verify that reconnect resumes with the last seen cursor ID."""
    received_after_values = []

    @async_retry_stream_with_backoff(max_retries=3, base_delay=0.01, max_delay=0.05)
    async def stream_with_cursor(workspace_id: str, after: str = None):
        received_after_values.append(after)
        if len(received_after_values) == 1:
            yield {"id": "evt-100", "payload": "event 1"}
            yield {"id": "evt-101", "payload": "event 2"}
            raise httpx.RemoteProtocolError("Connection reset by peer")
        yield {"id": "evt-102", "payload": "event 3"}

    results = []
    async for item in stream_with_cursor("ws-999"):
        results.append(item["id"])

    assert results == ["evt-100", "evt-101", "evt-102"]
    assert received_after_values == [None, "evt-101"]


@pytest.mark.asyncio
async def test_stream_object_cursor_tracking():
    """Verify that object attribute .id is tracked as cursor."""
    class EventObj:
        def __init__(self, event_id, name):
            self.id = event_id
            self.name = name

    reconnect_after = []

    @async_retry_stream_with_backoff(max_retries=2, base_delay=0.01, max_delay=0.05)
    async def object_stream(workspace_id: str, after: str = None):
        reconnect_after.append(after)
        if len(reconnect_after) == 1:
            yield EventObj("obj-42", "booting")
            raise httpx.ConnectError("Connection refused")
        yield EventObj("obj-43", "running")

    events = [item.id async for item in object_stream("ws-obj")]
    assert events == ["obj-42", "obj-43"]
    assert reconnect_after == [None, "obj-42"]


@pytest.mark.asyncio
async def test_stream_max_retries_exceeded():
    """Verify that exhausting retries raises the underlying network exception."""
    call_count = 0

    @async_retry_stream_with_backoff(max_retries=2, base_delay=0.01, max_delay=0.05)
    async def always_failing_stream(workspace_id: str, after: str = None):
        nonlocal call_count
        call_count += 1
        raise httpx.ReadTimeout("Persistent outage")

    with pytest.raises(httpx.ReadTimeout):
        async for _ in always_failing_stream("ws-fail"):
            pass

    # Initial call (1) + 2 retries = 3 calls
    assert call_count == 3


@pytest.mark.asyncio
async def test_non_retryable_exception_raises_immediately():
    """Verify that non-network errors are raised immediately without retries."""
    call_count = 0

    @async_retry_stream_with_backoff(max_retries=5, base_delay=0.01)
    async def faulty_stream(workspace_id: str, after: str = None):
        nonlocal call_count
        call_count += 1
        raise ValueError("Invalid schema")

    with pytest.raises(ValueError, match="Invalid schema"):
        async for _ in faulty_stream("ws-err"):
            pass

    assert call_count == 1


@pytest.mark.asyncio
async def test_positional_after_argument():
    """Verify that passing 'after' positionally does not cause TypeError on reconnect."""
    invocations = []

    @async_retry_stream_with_backoff(max_retries=2, base_delay=0.01, max_delay=0.05)
    async def stream_fn(workspace_id: str, after: str = None):
        invocations.append((workspace_id, after))
        if len(invocations) == 1:
            yield {"id": "pos-1"}
            raise httpx.ReadError("Socket closed")
        yield {"id": "pos-2"}

    # Pass "initial-pos" positionally
    events = [ev["id"] async for ev in stream_fn("ws-pos", "initial-pos")]
    assert events == ["pos-1", "pos-2"]
    assert invocations[0] == ("ws-pos", "initial-pos")
    assert invocations[1] == ("ws-pos", "pos-1")


@pytest.mark.asyncio
async def test_workspace_service_stream_mock():
    """Test WorkspaceService with a mock httpx streaming client."""
    class MockStreamContext:
        def __init__(self, lines):
            self.lines = lines

        async def __aenter__(self):
            mock_resp = MagicMock()
            mock_resp.raise_for_status = MagicMock()

            async def line_gen():
                for line in self.lines:
                    yield line

            mock_resp.aiter_lines = line_gen
            return mock_resp

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_client = MagicMock()
    mock_client.stream.return_value = MockStreamContext([
        json.dumps({"id": "ws-ev-1", "type": "WorkspaceStateChanged"}),
        "",  # Empty keep-alive line
        json.dumps({"id": "ws-ev-2", "type": "WorkspaceStarted"}),
    ])

    service = WorkspaceService(api_client=mock_client)
    events = []
    async for ev in service.get_workspace_events("workspace-alpha"):
        events.append(ev)

    assert len(events) == 2
    assert events[0]["id"] == "ws-ev-1"
    assert events[1]["id"] == "ws-ev-2"


@pytest.mark.asyncio
async def test_workspace_service_fallback():
    """Test WorkspaceService fallback when no api_client is provided."""
    service = WorkspaceService()
    events = []
    async for ev in service.get_workspace_events("ws-test"):
        events.append(ev)

    assert len(events) == 1
    assert events[0]["event"] == "stream_connected"
