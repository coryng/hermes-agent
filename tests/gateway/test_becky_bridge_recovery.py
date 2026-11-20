import asyncio

from gateway.run import GatewayRunner


def test_unconfigured_bridge_leaves_gateway_available(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    runner = object.__new__(GatewayRunner)
    runner._session_db = None
    asyncio.run(runner._start_becky_loops_bridge())
    assert getattr(runner, "_becky_loops_bridge", None) is None


def test_shortcut_dispatch_preserves_profile_and_reply_anchor():
    from types import SimpleNamespace
    from gateway.config import Platform

    class Adapter:
        async def handle_message(self, event):
            event._gateway_accepted = True
            self.event = event

    adapter = Adapter()
    runner = object.__new__(GatewayRunner)
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._primary_profile_name = "sanchez"
    runner.session_store = SimpleNamespace(lookup_by_session_id=lambda _: None)
    asyncio.run(runner._dispatch_becky_agent_reply(chat_id="123", thread_id="456", session_id="s1", text="hello", reply_to_message_id="789"))
    assert adapter.event.source.profile == "sanchez"
    assert runner._reply_anchor_for_event(adapter.event) == "789"


def test_two_bridges_start_and_isolate_authentication(monkeypatch):
    import json
    from types import SimpleNamespace
    import pytest
    from websockets.asyncio.client import connect
    from websockets.exceptions import InvalidStatus
    from gateway import becky_loops

    async def exercise():
        runners = []
        try:
            for chat_id, token in (("12345", "a" * 64), ("67890", "b" * 64)):
                config = becky_loops.BeckyLoopsConfig(enabled=True, chat_id=chat_id, token=token, port=0)
                monkeypatch.setattr(becky_loops, "load_becky_loops_config", lambda config=config: config)
                runner = object.__new__(GatewayRunner)
                runner._shutdown_event = asyncio.Event()
                runner._session_db = SimpleNamespace(_db=SimpleNamespace())
                runner.adapters = {}
                await runner._start_becky_loops_bridge()
                assert runner._becky_loops_bridge is not None
                runners.append(runner)
            first, second = (r._becky_loops_bridge.bound_port for r in runners)
            assert first != second
            async with connect(f"ws://127.0.0.1:{first}/api/ws?token={'a' * 64}") as client:
                ready = json.loads(await client.recv())
                assert ready["params"]["type"] == "gateway.ready"
            with pytest.raises(InvalidStatus):
                async with connect(f"ws://127.0.0.1:{second}/api/ws?token={'a' * 64}"):
                    pass
        finally:
            for runner in runners:
                await runner._stop_becky_loops_bridge()

    asyncio.run(exercise())


def test_shortcut_dispatch_fails_if_gateway_rejects_event():
    import pytest
    from types import SimpleNamespace
    from gateway.config import Platform

    class RejectingAdapter:
        async def handle_message(self, event):
            event._gateway_accepted = False

    runner = object.__new__(GatewayRunner)
    runner.adapters = {Platform.TELEGRAM: RejectingAdapter()}
    runner.session_store = SimpleNamespace(lookup_by_session_id=lambda _: None)
    with pytest.raises(RuntimeError, match="handoff"):
        asyncio.run(runner._dispatch_becky_agent_reply(chat_id="123", thread_id="456", session_id="s1", text="hello", reply_to_message_id="789"))


def test_shortcut_topic_recording_works_with_real_temporary_session_db(tmp_path):
    from gateway.becky_loops import SessionDBBeckyLoopsStore
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db")
    try:
        store = SessionDBBeckyLoopsStore(db)
        store._chat_id = "12345"
        session_id = store.record_shortcut_topic(title="Test", text="Hello", topic_id="7", message_id="9")
        assert session_id
        messages = db.get_messages(session_id, include_inactive=False)
        assert any(row.get("content") == "Hello" for row in messages)
    finally:
        db.close()



def test_shortcut_turn_registers_auto_close_with_actual_agent_result(monkeypatch, tmp_path):
    from unittest.mock import AsyncMock, MagicMock
    from tests.gateway.test_gateway_silence_tokens import _runner, _event

    runner = _runner(monkeypatch, tmp_path)
    result = {"final_response": "Done", "messages": [], "tools": [], "history_offset": 0,
              "last_prompt_tokens": 0, "api_calls": 1, "failed": False}
    runner._run_agent = AsyncMock(return_value=result)
    runner._register_becky_auto_close_after_delivery = MagicMock()
    event = _event(internal=True)
    event.metadata = {"becky_dashboard_new_topic": True, "becky_auto_close_policy": "simple_calendar_todoist_success"}
    asyncio.run(runner._handle_message_with_agent(event, event.source, "lane", 1))
    runner._register_becky_auto_close_after_delivery.assert_called_once()
    assert runner._register_becky_auto_close_after_delivery.call_args.kwargs["agent_result"] is result


def test_delivery_marker_reflects_success_before_callbacks(monkeypatch, tmp_path):
    from unittest.mock import AsyncMock
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from gateway.session import SessionSource
    from tests.gateway.test_diagnostic_wake_presentation import Adapter

    async def exercise(success, attachment_success=False, streamed=False, partial=False):
        adapter = Adapter(PlatformConfig(enabled=True), Platform.TELEGRAM)
        adapter.sent = []
        adapter._start_typing_refresh = lambda *a: None
        adapter._stop_typing_refresh = AsyncMock()
        adapter._run_processing_hook = AsyncMock()
        adapter._flush_text_debounce_now = AsyncMock()
        adapter._finish_session_task = lambda *a: None
        adapter.send_final_ledgered = AsyncMock(return_value=(SendResult(success=success, raw_response={"partial_overflow": partial}), adapter))
        event = MessageEvent(text="hello", source=SessionSource(platform=Platform.TELEGRAM, chat_id="123"), internal=True)
        observed = []
        async def check(*args):
            observed.append(getattr(event, "_hermes_delivery_succeeded", None))
        adapter._fire_post_delivery_callback = check
        async def handle(_event):
            if streamed:
                _event._hermes_delivery_succeeded = True
                return None
            return "answer"
        async def deliver_attachments(*args, record_delivery, **kwargs):
            if attachment_success:
                record_delivery(SendResult(success=True))
        adapter._deliver_attachments = deliver_attachments
        adapter._message_handler = handle
        await adapter._process_message_background(event, "session")
        assert observed == [(success and not partial) or streamed]
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    asyncio.run(exercise(True))
    asyncio.run(exercise(False, attachment_success=True))
    asyncio.run(exercise(False, streamed=True))
    asyncio.run(exercise(True, partial=True))


def test_confirmed_streaming_sets_delivery_marker(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from tests.gateway.test_gateway_silence_tokens import _runner, _event

    runner = _runner(monkeypatch, tmp_path)
    runner._delivery_adapter_for = lambda source: None
    event = _event(internal=True)
    result = {"already_sent": True, "failed": False}
    output = asyncio.run(runner._hmwa_deliver_turn_response(
        event, event.source, SimpleNamespace(session_id="synthetic"), "lane", 1,
        result, [], "Delivered", "", False,
    ))
    assert output is None
    assert event._hermes_delivery_succeeded is True
