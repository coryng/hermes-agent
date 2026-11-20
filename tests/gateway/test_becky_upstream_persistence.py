"""Bridge compatibility with the profile-scoped upstream persistence API."""
import asyncio

import pytest
from gateway import becky_loops
from gateway.run import GatewayRunner
from persistence_assembly import open_persistence, profile_scope


@pytest.mark.asyncio
async def test_bridge_starts_using_real_profile_scoped_persistence(tmp_path, monkeypatch):
    config = becky_loops.BeckyLoopsConfig(enabled=True, chat_id='100', token='0' * 64, port=0)
    monkeypatch.setattr(becky_loops, 'load_becky_loops_config', lambda: config)
    with open_persistence(profile_scope(home=tmp_path)) as db:
        runner = object.__new__(GatewayRunner)
        runner._session_db = db
        runner._shutdown_event = asyncio.Event()
        runner.adapters = {}
        try:
            await runner._start_becky_loops_bridge()
            assert runner._becky_loops_bridge is not None
        finally:
            await runner._stop_becky_loops_bridge()


def test_shortcut_projection_round_trip_is_isolated_between_profiles(tmp_path):
    with open_persistence(profile_scope(home=tmp_path / 'one')) as one, open_persistence(profile_scope(home=tmp_path / 'two')) as two:
        first = becky_loops.SessionDBBeckyLoopsStore(one)
        other = becky_loops.SessionDBBeckyLoopsStore(two)
        assert first.list_topics('100') == []
        session = first.record_shortcut_topic(title='Test topic', text='A question', topic_id='42', message_id='43')
        assert session
        first.record_shortcut_answer(session_id=session, text='An answer', message_id='44')
        rows = first.list_topics('100')
        assert len(rows) == 1 and rows[0]['title'] == 'Test topic'
        assert [m['content'] for m in first.transcript(session)] == ['A question', 'An answer']
        assert other.list_topics('100') == []
        assert first.end_topic_session(chat_id='100', thread_id='42', reason='telegram_topic_closed')
        assert first.list_topics('100')[0]['source_state'] == 'closed'


@pytest.mark.asyncio
async def test_close_command_finalizes_real_profile_session(tmp_path):
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    with open_persistence(profile_scope(home=tmp_path)) as db:
        store = becky_loops.SessionDBBeckyLoopsStore(db)
        store.list_topics('100')
        session = store.record_shortcut_topic(title='Close me', text='hello', topic_id='42', message_id='43')
        runner = object.__new__(GatewayRunner)
        runner._session_db = db
        runner._becky_loops_config = SimpleNamespace(chat_id='100', managed_topic_ids=set())
        runner._becky_loops_topic_controller = SimpleNamespace(is_connected=True, supports_close=True,
            close_topic=AsyncMock(return_value=datetime.now(UTC)))
        runner._notify_becky_topic_closed = AsyncMock(return_value='archived')
        await runner._handle_becky_close_command(chat_id='100', thread_id='42', user_id='7', message_id='8')
        assert db.sessions.get(session).as_session_row()['ended_at'] is not None


def test_topic_binding_lookup_uses_own_profile_name(tmp_path):
    from persistence.core.context import ProfileScope

    with open_persistence(ProfileScope(profile_dir=tmp_path / 'sanchez', name='sanchez')) as db:
        projection = becky_loops.SessionDBBeckyLoopsStore(db)
        projection.list_topics('100')
        session = projection.record_shortcut_topic(title='Bound', text='hello', topic_id='42', message_id='43')
        db.routing.bind_topic(chat_id='100', thread_id='42', user_id='7', session_key='example',
                              session_id=session, profile_name='sanchez')
        assert projection._db.get_telegram_topic_binding(chat_id='100', thread_id='42')['session_id'] == session
        assert projection.end_topic_session(chat_id='100', thread_id='42', reason='telegram_topic_closed')


@pytest.mark.asyncio
async def test_constructed_gateway_starts_bridge_with_profile_store(tmp_path, monkeypatch):
    from gateway.config import GatewayConfig
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    config = becky_loops.BeckyLoopsConfig(enabled=True, chat_id='100', token='0' * 64, port=0)
    monkeypatch.setattr(becky_loops, 'load_becky_loops_config', lambda: config)
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / 'sessions'))
    try:
        await runner._start_becky_loops_bridge()
        assert runner._becky_loops_bridge is not None
    finally:
        await runner._stop_becky_loops_bridge()
        runner.session_store.close_all_db_handles()
        runner.close_all_session_db_handles()


def test_real_gateway_session_store_records_shortcut_and_routes_topic(tmp_path, monkeypatch):
    from gateway.config import GatewayConfig
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / 'sessions'))
    try:
        projection = becky_loops.SessionDBBeckyLoopsStore(runner._session_db, session_store=runner.session_store)
        projection.list_topics('100')
        session = projection.record_shortcut_topic(title='Real route', text='A question', topic_id='42', message_id='43')
        assert session
        projection.record_shortcut_answer(session_id=session, text='An answer', message_id='44')
        assert [m['content'] for m in projection.transcript(session)] == ['A question', 'An answer']
        row = projection.list_topics('100')[0]
        assert row['session_id'] == session and row['title'] == 'Real route'
        entry = runner.session_store.lookup_by_session_id(session)
        assert entry is not None
        runner._record_telegram_topic_binding(projection._shortcut_source('42'), entry)
        assert projection._db.get_telegram_topic_binding(chat_id='100', thread_id='42')['session_id'] == session
    finally:
        runner.session_store.close_all_db_handles()
        runner.close_all_session_db_handles()
