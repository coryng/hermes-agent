"""Regressions for the private Shortcut bridge retaining a dead Telegram adapter."""

import asyncio
import json
from types import SimpleNamespace

import pytest
from websockets.asyncio.client import connect

from gateway import becky_loops
from gateway.config import Platform
from gateway.run import GatewayRunner


class TelegramTransport:
    def __init__(self):
        self.is_connected = True
        self.sent = []
        self.closed = []
        self.handler = None
        self._bot = SimpleNamespace(close_forum_topic=self.close_topic)

    def set_becky_close_command_handler(self, handler):
        self.handler = handler

    async def send(self, **kwargs):
        assert self.is_connected
        self.sent.append(kwargs)
        return SimpleNamespace(success=True, message_id="42", raw_response={})

    async def close_topic(self, **kwargs):
        assert self.is_connected
        self.closed.append(kwargs)
        return True


def runner_with(adapter):
    runner = object.__new__(GatewayRunner)
    runner._shutdown_event = asyncio.Event()
    runner._session_db = SimpleNamespace(_db=SimpleNamespace())
    runner.adapters = {} if adapter is None else {Platform.TELEGRAM: adapter}
    runner._sync_voice_mode_state_to_adapter = lambda _: None
    runner._bind_voice_input_callback = lambda _: None
    return runner


async def capabilities(bridge, token):
    async with connect(
        f"ws://127.0.0.1:{bridge.bound_port}/api/ws?token={token}", proxy=None
    ) as client:
        await client.recv()
        await client.send(json.dumps({
            "jsonrpc": "2.0", "id": 1,
            "method": "becky.loops.capabilities", "params": {},
        }))
        return json.loads(await client.recv())["result"]


@pytest.mark.asyncio
async def test_replacement_during_bridge_start_is_not_missed(monkeypatch):
    config = becky_loops.BeckyLoopsConfig(enabled=True, chat_id="100", token="0" * 64,
        port=0, topic_reply="bot_api_private_topic", topic_control="bot_api_private_topic")
    monkeypatch.setattr(becky_loops, "load_becky_loops_config", lambda: config)
    original_start = becky_loops.start_becky_loops_bridge
    runner = runner_with(TelegramTransport())
    fresh = TelegramTransport()

    async def start_with_reconnect(**kwargs):
        runner._publish_primary_adapter(Platform.TELEGRAM, fresh)
        return await original_start(**kwargs)

    monkeypatch.setattr(becky_loops, "start_becky_loops_bridge", start_with_reconnect)
    try:
        await runner._start_becky_loops_bridge()
        await runner._becky_loops_bridge.topic_sender.send_topic(
            chat_id="100", thread_id="7", text="test", reply_to_message_id="9")
        assert len(fresh.sent) == 1
    finally:
        await runner._stop_becky_loops_bridge()


@pytest.mark.asyncio
@pytest.mark.parametrize("initially_connected", [True, False])
async def test_bridge_delivery_follows_replacement_without_crossing_profiles(
    monkeypatch, initially_connected
):
    runners = []
    try:
        for index, profile in enumerate(("becky", "sanchez")):
            config = becky_loops.BeckyLoopsConfig(
                enabled=True, chat_id=str(100 + index), token=str(index) * 64,
                port=0, topic_reply="bot_api_private_topic",
                topic_control="bot_api_private_topic",
            )
            monkeypatch.setattr(becky_loops, "load_becky_loops_config", lambda: config)
            runner = runner_with(TelegramTransport() if initially_connected else None)
            runner._primary_profile_name = profile
            await runner._start_becky_loops_bridge()
            runners.append(runner)

        for index, runner in enumerate(runners):
            bridge = runner._becky_loops_bridge
            pending = bridge._new_topic_answers
            pending["retained"] = ("payload", asyncio.get_running_loop().create_future())
            for _ in range(2):
                previous = runner.adapters.pop(Platform.TELEGRAM, None)
                if previous is not None:
                    previous.is_connected = False
                assert (await capabilities(bridge, str(index) * 64))["topic_reply"] == "unavailable"
                fresh = TelegramTransport()
                runner._publish_primary_adapter(Platform.TELEGRAM, fresh)
                assert runner._becky_loops_bridge is bridge
                assert bridge._new_topic_answers is pending and "retained" in pending
                assert (await capabilities(bridge, str(index) * 64))["topic_reply"] == "bot_api_private_topic"
                assert fresh.handler.__self__ is runner
                await bridge.topic_sender.send_topic(
                    chat_id=str(100 + index), thread_id="7", text="synthetic reply",
                    reply_to_message_id="9",
                )
                await bridge.topic_controller.close_topic(chat_id=str(100 + index), thread_id="7")
                assert [m["chat_id"] for m in fresh.sent] == [str(100 + index)]
                assert fresh.closed == [{"chat_id": 100 + index, "message_thread_id": 7}]
                other = runners[1 - index]._becky_loops_bridge
                assert other.topic_sender is not bridge.topic_sender
    finally:
        for runner in runners:
            await runner._stop_becky_loops_bridge()
