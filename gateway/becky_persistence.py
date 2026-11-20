"""Profile-scoped persistence operations used by the Shortcut bridge."""

from dataclasses import asdict

from persistence.conversation.messages import NewMessage, TranscriptRead, TranscriptView
from persistence.conversation.sessions import NewSession, SessionOrder, SessionQuery, SessionWindow


class BeckyPersistence:
    """Translate the bridge's narrow storage needs to upstream repositories."""

    def __init__(self, store):
        self.store = store

    def list_sessions_rich(self, *, source, include_children, include_archived,
                           project_compression_tips, order_by_last_active, limit, offset):
        query = SessionQuery(sources=(source,), include_children=include_children,
            include_archived=include_archived, project_compression_tips=project_compression_tips,
            order=SessionOrder.LAST_ACTIVE if order_by_last_active else SessionOrder.STARTED_AT)
        return [row.as_list_row() for row in self.store.sessions.list_sessions(
            query, SessionWindow(limit=limit, offset=offset))]

    def get_session(self, session_id):
        row = self.store.sessions.get(session_id)
        return None if row is None else row.as_session_row()

    def create_session(self, session_id, source, **kwargs):
        return self.store.sessions.create(NewSession(session_id=session_id, source=source, **kwargs))

    def end_session(self, session_id, reason):
        self.store.sessions.end(session_id, reason)

    def set_session_title(self, session_id, title):
        return self.store.sessions.set_title(session_id, title)

    def get_messages(self, session_id, *, include_inactive=False):
        view = TranscriptView.ALL if include_inactive else TranscriptView.ACTIVE
        return [row.as_wire_row() for row in self.store.messages.read_transcript(session_id, TranscriptRead(view=view))]

    def append_message(self, session_id, role, **kwargs):
        return self.store.messages.append(session_id, NewMessage.from_live({'role': role, **kwargs}))

    def get_telegram_topic_binding(self, *, chat_id, thread_id):
        binding = self.store.routing.get_topic_binding(chat_id=chat_id, thread_id=thread_id,
                                                       profile_name=self.store.scope.name)
        return None if binding is None else asdict(binding)
