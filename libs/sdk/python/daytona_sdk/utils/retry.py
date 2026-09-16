import asyncio
import functools
import inspect
import logging
import random
from typing import Any, AsyncGenerator, Callable, Optional, Tuple, Type

logger = logging.getLogger("daytona_sdk.retry")

DEFAULT_RETRYABLE_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    asyncio.TimeoutError,
)

try:
    import httpx

    DEFAULT_RETRYABLE_EXCEPTIONS = (
        asyncio.TimeoutError,
        httpx.ReadTimeout,
        httpx.ConnectError,
        httpx.RemoteProtocolError,
        httpx.ReadError,
    )
except ImportError:
    pass


def _extract_cursor(item: Any) -> Optional[str]:
    cursor_keys = ("id", "event_id", "last_event_id", "cursor", "timestamp")
    if isinstance(item, dict):
        for key in cursor_keys:
            if key in item and item[key] is not None:
                return str(item[key])
    else:
        for key in cursor_keys:
            if hasattr(item, key):
                val = getattr(item, key)
                if val is not None:
                    return str(val)
    return None


def async_retry_stream_with_backoff(
    max_retries: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = DEFAULT_RETRYABLE_EXCEPTIONS,
):
    """Decorator that wraps an async generator with exponential backoff and jitter.

    Implements full-jitter exponential backoff:
    T_sleep = uniform(0, min(T_max, T_base * 2^(attempt - 1)))
    Automatically tracks event cursor ('after') across reconnects to prevent
    duplicate event delivery.

    Args:
        max_retries: Maximum number of consecutive reconnect attempts before giving up.
        base_delay: Initial retry backoff delay in seconds.
        max_delay: Maximum retry backoff delay cap in seconds.
        retryable_exceptions: Tuple of exception types that should trigger a retry.
    """

    def decorator(agen_func: Callable[..., AsyncGenerator[Any, None]]):
        sig = inspect.signature(agen_func)
        param_names = list(sig.parameters.keys())
        after_pos = param_names.index("after") if "after" in param_names else -1
        has_var_keyword = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )

        @functools.wraps(agen_func)
        async def wrapper(*args, **kwargs) -> AsyncGenerator[Any, None]:
            attempt = 0
            args_list = list(args)

            # Retrieve initial cursor from kwargs or positional args if present
            last_cursor: Optional[str] = kwargs.get("after")
            if last_cursor is None and after_pos != -1 and len(args_list) > after_pos:
                last_cursor = args_list[after_pos]

            while True:
                try:
                    # Update cursor in call arguments
                    if last_cursor is not None:
                        if after_pos != -1 and len(args_list) > after_pos:
                            args_list[after_pos] = last_cursor
                        elif after_pos != -1 or has_var_keyword or "after" in kwargs:
                            kwargs["after"] = last_cursor

                    res = agen_func(*args_list, **kwargs)
                    if inspect.isawaitable(res):
                        res = await res

                    if res is not None:
                        if hasattr(res, "__aiter__"):
                            async for item in res:
                                attempt = 0
                                cursor = _extract_cursor(item)
                                if cursor is not None:
                                    last_cursor = cursor
                                yield item
                        elif hasattr(res, "__iter__"):
                            for item in res:
                                attempt = 0
                                cursor = _extract_cursor(item)
                                if cursor is not None:
                                    last_cursor = cursor
                                yield item
                        else:
                            attempt = 0
                            cursor = _extract_cursor(res)
                            if cursor is not None:
                                last_cursor = cursor
                            yield res
                    return

                except retryable_exceptions as exc:
                    attempt += 1
                    if attempt > max_retries:
                        logger.error(
                            f"Exceeded max retries ({max_retries}) for event stream: {exc}"
                        )
                        raise

                    # Exponential backoff with full jitter
                    delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                    jittered_delay = random.uniform(0.0, delay)

                    logger.warning(
                        f"Event stream disconnected ({exc.__class__.__name__}). "
                        f"Retrying {attempt}/{max_retries} in {jittered_delay:.2f}s..."
                    )
                    await asyncio.sleep(jittered_delay)

        return wrapper

    return decorator
