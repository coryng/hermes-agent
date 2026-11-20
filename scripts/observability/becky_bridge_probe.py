"""Check the private Shortcut handoff without sending messages or invoking a model."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
from urllib.parse import quote, urlsplit

from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus, WebSocketException

PUBLIC_METHODS = {"list", "summarize", "close", "reopen", "reply", "reply_retry"}


class NoRedirectConnect(connect):
    def process_redirect(self, exc):
        return exc


async def probe(url: str, token: str, timeout: float = 5.0) -> dict:
    """Return only bounded, non-secret health information for a loopback bridge."""
    try:
        address = urlsplit(url)
        valid = (
            address.scheme == "ws" and address.hostname == "127.0.0.1"
            and address.port is not None and address.path == "/api/ws"
            and not any((address.username, address.password, address.query, address.fragment))
        )
    except ValueError:
        valid = False
    if not valid:
        return {"ready": False, "error": "invalid_address"}
    try:
        async with asyncio.timeout(timeout):
            async with NoRedirectConnect(
                f"{url}?token={quote(token, safe='')}", proxy=None,
                open_timeout=timeout, close_timeout=1, max_size=262144,
            ) as client:
                frame = await client.recv()
                if not isinstance(frame, str):
                    return {"ready": False, "error": "protocol"}
                ready = json.loads(frame)
                if ready != {"jsonrpc": "2.0", "method": "event", "params": {
                    "type": "gateway.ready", "payload": {"skin": {}}
                }}:
                    return {"ready": False, "error": "protocol"}
                await client.send(json.dumps({
                    "jsonrpc": "2.0", "id": 1,
                    "method": "becky.loops.capabilities", "params": {},
                }))
                frame = await client.recv()
                if not isinstance(frame, str):
                    return {"ready": False, "error": "protocol"}
                response = json.loads(frame)
        if not isinstance(response, dict) or set(response) != {"jsonrpc", "id", "result"} or type(response.get("id")) is not int or response.get("id") != 1 or response.get("jsonrpc") != "2.0":
            return {"ready": False, "error": "protocol"}
        result = response.get("result")
        if not isinstance(result, dict):
            return {"ready": False, "error": "protocol"}
        methods = result.get("methods")
        compatible = (
            result.get("schema_version") == "2" and result.get("summary_schema_version") == "1"
            and isinstance(methods, list) and len(methods) == len(PUBLIC_METHODS)
            and all(isinstance(method, str) for method in methods)
            and set(methods) == PUBLIC_METHODS
            and set(result) == {"schema_version", "summary_schema_version", "methods", "topic_control", "topic_reply", "same_topic_reopen", "new_session_fallback", "max_request_bytes", "max_response_bytes"}
            and result.get("topic_control") in {"unavailable", "bot_api_private_topic", "mtproto_private_topic"}
            and result.get("topic_reply") in {"unavailable", "bot_api_private_topic"}
            and type(result.get("same_topic_reopen")) is bool
            and type(result.get("new_session_fallback")) is bool
            and type(result.get("max_request_bytes")) is int and result.get("max_request_bytes") == 65536
            and type(result.get("max_response_bytes")) is int and result.get("max_response_bytes") == 262144
        )
        reply_ready = result.get("topic_reply") == "bot_api_private_topic"
        error = None if reply_ready else "topic_reply_unavailable"
        if not compatible:
            error = "capability_mismatch"
        return {
            "ready": error is None, "error": error,
            "topic_reply_ready": reply_ready, "capabilities_compatible": compatible,
            "topic_control_ready": result.get("topic_control") in {
                "bot_api_private_topic", "mtproto_private_topic"
            },
        }
    except InvalidStatus:
        return {"ready": False, "error": "handshake_rejected"}
    except TimeoutError:
        return {"ready": False, "error": "timeout"}
    except (OSError, WebSocketException):
        return {"ready": False, "error": "unreachable"}
    except (ValueError, TypeError):
        return {"ready": False, "error": "protocol"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hermes-home", type=Path, required=True,
                        help="The explicit profile home containing config.yaml and .env")
    args = parser.parse_args()
    from dotenv import dotenv_values
    import yaml

    try:
        config = yaml.safe_load((args.hermes_home / "config.yaml").read_text(encoding="utf-8-sig"))
        section = config.get("gateway", {}).get("becky_loops", {})
        token = dotenv_values(args.hermes_home / ".env", interpolate=False).get("HERMES_BECKY_LOOPS_TOKEN")
        if section.get("enabled") is not True or not token:
            result = {"ready": False, "error": "not_configured"}
        else:
            port = int(section.get("port", 9120))
            result = asyncio.run(probe(f"ws://127.0.0.1:{port}/api/ws", token))
    except (OSError, ValueError, TypeError, AttributeError, yaml.YAMLError):
        result = {"ready": False, "error": "invalid_config"}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    sys.exit(main())
