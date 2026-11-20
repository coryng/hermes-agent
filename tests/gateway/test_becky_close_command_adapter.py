from __future__ import annotations

from types import SimpleNamespace

import pytest

from plugins.platforms.telegram.adapter import TelegramAdapter


@pytest.mark.asyncio
async def test_close_command_is_not_sent_to_the_llm() -> None:
    adapter = object.__new__(TelegramAdapter)
    calls: list[dict[str, str]] = []
    sent: list[dict[str, object]] = []

    async def close_handler(**kwargs: str) -> str:
        calls.append(kwargs)
        return "Topic closed."

    adapter.set_becky_close_command_handler(close_handler)
    adapter._effective_update_message = lambda update: update.message
    adapter._is_user_authorized_from_message = lambda message: True
    async def capture_send(*args: object, **kwargs: object) -> object:
        del args
        sent.append(kwargs)
        return SimpleNamespace(success=True)

    adapter.send = capture_send
    adapter._should_process_message = lambda *args, **kwargs: True
    adapter._source_from_message_for_auth = lambda message: SimpleNamespace(user_id="1234567890", profile_route_rejected=False)
    adapter._canonicalize = lambda source: None
    adapter.gateway_runner = SimpleNamespace(_is_user_authorized_for_source=lambda source: True)

    message = SimpleNamespace(
        text="/close",
        chat=SimpleNamespace(id=1234567890, type="private", is_forum=False),
        message_thread_id=3,
        is_topic_message=True,
        from_user=SimpleNamespace(id=1234567890),
        message_id=42,
    )
    update = SimpleNamespace(message=message, update_id=7)

    await adapter._handle_command(update, None)

    assert calls == [
        {
            "chat_id": "1234567890",
            "thread_id": "3",
            "user_id": "1234567890",
            "message_id": "42",
        }
    ]
    assert sent[0]["metadata"] == {
        "thread_id": "3",
        "notify": True,
        "telegram_dm_topic_reply_fallback": True,
        "direct_messages_topic_id": "3",
        "telegram_reply_to_message_id": "42",
    }


def test_close_command_only_accepts_this_bot_target() -> None:
    assert TelegramAdapter._is_becky_close_command("/close") is True
    assert TelegramAdapter._is_becky_close_command(
        "/close@becky_bot", "becky_bot"
    ) is True
    assert TelegramAdapter._is_becky_close_command(
        "/close@another_bot", "becky_bot"
    ) is False
    assert TelegramAdapter._is_becky_close_command("/close@becky_bot") is False


async def _async_noop_send(*args: object, **kwargs: object) -> object:
    del args, kwargs
    return SimpleNamespace(success=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("user_id,authorized", [(None, True), (111, False)])
async def test_close_fails_closed_when_pairing_prefilter_allows_unauthorized_user(user_id, authorized):
    adapter = object.__new__(TelegramAdapter)
    calls = []
    async def handler(**kwargs):
        calls.append(kwargs)
        return "closed"
    adapter.set_becky_close_command_handler(handler)
    adapter._effective_update_message = lambda update: update.message
    adapter._is_user_authorized_from_message = lambda message: True
    adapter._should_process_message = lambda *args, **kwargs: True
    adapter._canonicalize = lambda source: None
    adapter._source_from_message_for_auth = lambda message: SimpleNamespace(user_id=str(user_id) if user_id else None)
    adapter.gateway_runner = SimpleNamespace(_is_user_authorized_for_source=lambda source: authorized)
    msg = SimpleNamespace(text="/close", chat=SimpleNamespace(id=123), from_user=SimpleNamespace(id=user_id) if user_id else None, message_id=42, message_thread_id=3)
    await adapter._handle_command(SimpleNamespace(message=msg), None)
    assert calls == []


@pytest.mark.asyncio
async def test_close_obeys_configured_telegram_chat_gate():
    adapter = object.__new__(TelegramAdapter)
    calls = []
    async def handler(**kwargs):
        calls.append(kwargs)
        return "closed"
    adapter.set_becky_close_command_handler(handler)
    adapter._effective_update_message = lambda update: update.message
    adapter._is_user_authorized_from_message = lambda message: True
    adapter._should_process_message = lambda *args, **kwargs: False
    msg = SimpleNamespace(text="/close", chat=SimpleNamespace(id=123), from_user=SimpleNamespace(id=111), message_id=42, message_thread_id=3)
    await adapter._handle_command(SimpleNamespace(message=msg), None)
    assert calls == []
