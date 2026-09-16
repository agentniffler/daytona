import json
import logging
from typing import Any, AsyncGenerator, Dict, Optional
import httpx

from daytona_sdk.utils.retry import async_retry_stream_with_backoff

logger = logging.getLogger("daytona_sdk.services.workspace")


class WorkspaceService:
    def __init__(self, api_client: Any = None):
        self.api_client = api_client

    @async_retry_stream_with_backoff(max_retries=5, base_delay=0.5, max_delay=30.0)
    async def get_workspace_events(
        self, workspace_id: str, after: Optional[str] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream workspace events with automated reconnection and exponential backoff with jitter.

        Args:
            workspace_id: The ID of the workspace to stream events for.
            after: Optional cursor / event ID to resume streaming from.

        Yields:
            Dict containing the event payload.
        """
        url = f"/workspaces/{workspace_id}/events"
        params: Dict[str, str] = {}
        if after is not None:
            params["after"] = str(after)

        if self.api_client is not None:
            if hasattr(self.api_client, "stream"):
                async with self.api_client.stream("GET", url, params=params) as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith("data:"):
                            line = line[len("data:"):].strip()
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError:
                            yield {"raw": line}
            elif hasattr(self.api_client, "get_workspace_events"):
                async for event in self.api_client.get_workspace_events(
                    workspace_id=workspace_id, after=after
                ):
                    yield event
            elif hasattr(self.api_client, "get"):
                response = await self.api_client.get(url, params=params)
                data = response.json()
                if isinstance(data, list):
                    for item in data:
                        yield item
                else:
                    yield data
        else:
            yield {"id": "1", "event": "stream_connected", "workspace_id": workspace_id}
