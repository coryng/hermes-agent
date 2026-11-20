"""The health probe must detect a live-but-unusable bridge without doing work."""

import json

import pytest
from websockets.asyncio.server import serve


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("topic_reply", "extra_method", "ready", "error"),
    [("bot_api_private_topic", False, True, None),
     ("unavailable", False, False, "topic_reply_unavailable"),
     ("bot_api_private_topic", True, False, "capability_mismatch")],
)
async def test_probe_checks_reply_readiness_and_deployed_contract(
    topic_reply, extra_method, ready, error
):
    from scripts.observability.becky_bridge_probe import probe

    calls = []

    async def handle(connection):
        await connection.send(json.dumps({
            "jsonrpc": "2.0", "method": "event",
            "params": {"type": "gateway.ready", "payload": {"skin": {}}},
        }))
        request = json.loads(await connection.recv())
        calls.append(request)
        methods = ["list", "summarize", "close", "reopen", "reply", "reply_retry"]
        if extra_method:
            methods.append("unexpected_operation")
        await connection.send(json.dumps({
            "jsonrpc": "2.0", "id": request["id"], "result": {
                "schema_version": "2", "summary_schema_version": "1",
                "methods": methods, "topic_reply": topic_reply,
                "topic_control": "unavailable", "same_topic_reopen": False,
                "new_session_fallback": True, "max_request_bytes": 65536,
                "max_response_bytes": 262144,
            },
        }))

    async with serve(handle, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        result = await probe(f"ws://127.0.0.1:{port}/api/ws", "synthetic-token")
    assert result["ready"] is ready
    assert result["error"] == error
    assert calls == [{"jsonrpc": "2.0", "id": 1,
                      "method": "becky.loops.capabilities", "params": {}}]
    assert "synthetic-token" not in json.dumps(result)


@pytest.mark.asyncio
async def test_probe_refuses_remote_address_before_sending_credentials():
    from scripts.observability.becky_bridge_probe import probe

    result = await probe("wss://example.com/api/ws", "synthetic-secret")
    assert result == {"ready": False, "error": "invalid_address"}


@pytest.mark.asyncio
async def test_probe_rejects_binary_greeting():
    from scripts.observability.becky_bridge_probe import probe

    async def handle(connection):
        await connection.send(b'{}')

    async with serve(handle, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        result = await probe(f"ws://127.0.0.1:{port}/api/ws", "synthetic-token")
    assert result == {"ready": False, "error": "protocol"}
