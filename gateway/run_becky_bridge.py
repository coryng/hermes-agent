from __future__ import annotations

import asyncio
import logging
import ipaddress
import os
from datetime import datetime
from urllib.parse import urlsplit

from gateway.config import Platform
from gateway.platforms.base import MessageEvent, MessageType
from gateway.session import SessionSource

logger = logging.getLogger(__name__)

def _validated_becky_dashboard_url(raw_url: object) -> str | None:
    """Accept only an HTTP(S) URL whose literal host is a loopback IP."""
    if not isinstance(raw_url, str) or not raw_url.strip():
        return None
    value = raw_url.strip()
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or host is None
        or "%" in host
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        return None
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return None
    if not address.is_loopback:
        return None
    if port is not None and not 1 <= port <= 65_535:
        return None
    return value.rstrip("/")


class GatewayBeckyBridgeMixin:

    def _refresh_becky_telegram_adapter(self, adapter) -> None:
        """Rebind transport after reconnect without losing pending bridge requests."""
        bridge = getattr(self, "_becky_loops_bridge", None)
        if bridge is None:
            return
        from gateway.becky_loops import TelegramTopicController, TelegramTopicSender
        from gateway.becky_loop_reply import AsyncAuxiliaryReplyProvider, LoopReplyGenerator

        setter = getattr(adapter, "set_becky_close_command_handler", None)
        if callable(setter):
            setter(self._handle_becky_close_command)
        if bridge.config.topic_reply == "bot_api_private_topic":
            bridge.topic_sender = TelegramTopicSender(adapter)
            bridge.agent_dispatcher = self._dispatch_becky_agent_reply
            if bridge.reply_generator is None:
                bridge.reply_generator = LoopReplyGenerator(AsyncAuxiliaryReplyProvider())
        if bridge.config.topic_control == "bot_api_private_topic":
            controller = TelegramTopicController(adapter)
            bridge.topic_controller = controller
            self._becky_loops_topic_controller = controller

    async def _dispatch_becky_agent_reply(
        self,
        *,
        chat_id: str,
        thread_id: str,
        session_id: str,
        text: str,
        reply_to_message_id: str,
        auto_close_policy: str | None = None,
        new_topic: bool = False,
    ) -> None:
        """Queue an app-originated comment through the real Telegram agent.

        The loop bridge has already delivered the visible ``Cory via Becky``
        comment.  This synthetic event is the authenticated handoff into the
        normal Telegram session pipeline, so the existing topic session,
        tools, approvals, and transcript persistence all remain authoritative.
        ``BasePlatformAdapter.handle_message`` intentionally returns after
        scheduling the agent work; the resulting answer is delivered by the
        ordinary Telegram adapter in the same topic.
        """
        adapter = self.adapters.get(Platform.TELEGRAM)
        handle_message = getattr(adapter, "handle_message", None)
        if not callable(handle_message):
            raise RuntimeError("Telegram agent adapter is unavailable")
        chat_type = "group" if str(chat_id).startswith("-") else "dm"
        import uuid as _uuid

        session_store = getattr(self, "session_store", None)
        lookup = getattr(session_store, "lookup_by_session_id", None)
        session_entry = lookup(session_id) if callable(lookup) else None
        origin = getattr(session_entry, "origin", None)
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=SessionSource(
                platform=Platform.TELEGRAM,
                chat_id=str(chat_id),
                chat_type=chat_type,
                user_id=getattr(origin, "user_id", None),
                user_name=getattr(origin, "user_name", None),
                user_id_alt=getattr(origin, "user_id_alt", None),
                thread_id=str(thread_id),
                profile=getattr(origin, "profile", None) or getattr(self, "_primary_profile_name", None),
            ),
            message_id=f"becky-dashboard-{_uuid.uuid4().hex}",
            reply_to_message_id=str(reply_to_message_id),
            metadata={
                "becky_dashboard_reply": True,
                **(
                    {"becky_dashboard_new_topic": True}
                    if new_topic
                    else {}
                ),
                **(
                    {"becky_auto_close_policy": auto_close_policy}
                    if auto_close_policy is not None
                    else {}
                ),
            },
            internal=True,
        )
        if chat_type == "dm":
            event.reply_anchor_override = str(reply_to_message_id)
        source = event.source
        if session_entry is not None:
            try:
                await asyncio.to_thread(
                    self._record_telegram_topic_binding, source, session_entry
                )
            except Exception:
                logger.debug(
                    "Unable to bind dashboard reply to its source session",
                    exc_info=True,
                )
        try:
            await handle_message(event)
            if getattr(event, "_gateway_accepted", None) is False:
                raise RuntimeError("Telegram handoff rejected")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The loop bridge deliberately converts dispatcher failures into a
            # safe ``answer_unavailable`` result. Keep the public response
            # generic, but preserve the exception type/stack in the local
            # gateway log so a broken agent handoff is diagnosable.
            logger.error(
                "Becky dashboard reply handoff failed before agent scheduling (%s)",
                type(exc).__name__,
                exc_info=True,
            )
            raise

    def _register_becky_auto_close_after_delivery(
        self,
        *,
        event: MessageEvent,
        source: SessionSource,
        session_key: str,
        run_generation: int | None,
        agent_result: object,
    ) -> None:
        """Schedule a narrow Becky topic close after the answer is delivered."""
        from gateway.becky_loops import (
            BECKY_AUTO_CLOSE_POLICY_SIMPLE_CALENDAR_TODOIST_SUCCESS,
            should_auto_close_becky_loop,
        )

        metadata = getattr(event, "metadata", None)
        if (
            not isinstance(metadata, dict)
            or metadata.get("becky_dashboard_new_topic") is not True
            or metadata.get("becky_auto_close_policy")
            != BECKY_AUTO_CLOSE_POLICY_SIMPLE_CALENDAR_TODOIST_SUCCESS
        ):
            return
        if not isinstance(run_generation, int) or run_generation < 1:
            return
        if not should_auto_close_becky_loop(agent_result):
            return

        adapter = self.adapters.get(source.platform)
        register = getattr(adapter, "register_post_delivery_callback", None)
        if not callable(register):
            logger.info(
                "Skipping Becky auto-close because the Telegram adapter has no "
                "post-delivery callback support"
            )
            return
        async def _close_after_delivery() -> None:
            # BasePlatformAdapter annotates the event immediately before it
            # invokes deferred callbacks. A custom adapter that invokes this
            # callback early or a cancelled/failed send must leave the topic
            # open.
            if getattr(event, "_hermes_delivery_succeeded", None) is not True:
                return
            is_current = getattr(self, "_is_session_run_current", None)
            if not callable(is_current):
                return
            if not is_current(session_key, run_generation):
                return
            pending_messages = getattr(adapter, "_pending_messages", None)
            if isinstance(pending_messages, dict) and session_key in pending_messages:
                logger.info(
                    "Skipping Becky auto-close because a follow-up is pending for %s",
                    session_key,
                )
                return
            session_tasks = getattr(adapter, "_session_tasks", None)
            current_task = asyncio.current_task()
            owner_task = (
                session_tasks.get(session_key)
                if isinstance(session_tasks, dict)
                else None
            )
            owner_done = getattr(owner_task, "done", None)
            if (
                owner_task is not None
                and owner_task is not current_task
                and (not callable(owner_done) or not owner_done())
            ):
                logger.info(
                    "Skipping Becky auto-close because a newer session task owns %s",
                    session_key,
                )
                return
            await self._maybe_auto_close_becky_topic(
                source=source,
                agent_result=agent_result,
            )

        try:
            register(
                session_key,
                _close_after_delivery,
                generation=run_generation,
            )
        except Exception:
            # Callback registration is a safety boundary. If the adapter
            # cannot guarantee response-before-close ordering, leave the loop
            # open rather than closing it early.
            logger.warning(
                "Unable to register Becky auto-close post-delivery callback",
                exc_info=True,
            )

    async def _maybe_auto_close_becky_topic(
        self,
        *,
        source: SessionSource,
        agent_result: object,
    ) -> None:
        """Close a Becky topic only when the narrow success predicate passes."""
        from gateway.becky_loops import should_auto_close_becky_loop

        if not should_auto_close_becky_loop(agent_result):
            return
        try:
            await self._handle_becky_close_command(
                chat_id=str(source.chat_id),
                thread_id=str(source.thread_id or ""),
                user_id=str(source.user_id or ""),
                message_id="",
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # The close command itself is fail-safe, but keep this boundary
            # defensive so a controller/configuration error never affects the
            # already-delivered assistant response.
            logger.warning("Becky automatic topic close failed", exc_info=True)

    async def _handle_becky_close_command(
        self,
        *,
        chat_id: str,
        thread_id: str,
        user_id: str,
        message_id: str,
    ) -> str:
        """Close one Telegram topic without entering the LLM pipeline."""
        del user_id, message_id
        config = getattr(self, "_becky_loops_config", None)
        if config is None or str(chat_id) != str(getattr(config, "chat_id", "")):
            return "This topic is not managed by Becky."
        managed = getattr(config, "managed_topic_ids", frozenset())
        if not thread_id or thread_id == "1" or thread_id in managed:
            return "This is a Hermes system topic and cannot be closed as a loop."
        controller = getattr(self, "_becky_loops_topic_controller", None)
        if controller is None or not bool(getattr(controller, "is_connected", False)):
            return "Topic close is unavailable."
        if not bool(getattr(controller, "supports_close", False)):
            return "Topic close is unavailable."

        from datetime import UTC, datetime

        try:
            closed_at = await controller.close_topic(
                chat_id=str(chat_id), thread_id=str(thread_id)
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if getattr(error, "code", None) != "topic_already_closed":
                logger.warning("Telegram /close topic control failed", exc_info=True)
                return "Topic close is unavailable."
            closed_at = datetime.now(UTC)
        if closed_at.tzinfo is None or closed_at.utcoffset() is None:
            closed_at = closed_at.replace(tzinfo=UTC)

        session_ids: list[str] = []
        session_db = getattr(self, "_session_db", None)
        if session_db is not None:
            try:
                binding = await session_db.get_telegram_topic_binding(
                    chat_id=str(chat_id), thread_id=str(thread_id)
                )
                if binding:
                    bound_id = str(binding.get("session_id") or "").strip()
                    if bound_id and bound_id not in session_ids:
                        session_ids.append(bound_id)
                # Older Telegram conversations (and topics created outside
                # the Shortcut flow) may not have a binding row.  The topic
                # tuple is still authoritative.  Collect every active row for
                # the tuple: compression/restart history can leave duplicate
                # sessions, and ending only the newest one makes the topic
                # reappear as active on the next projection refresh.
                list_sessions = getattr(session_db, "list_sessions_rich", None)
                if callable(list_sessions):
                    from gateway.becky_loops import (
                        _MAX_SESSION_SCAN,
                        _SESSION_PAGE_SIZE,
                    )

                    offset = 0
                    scanned = 0
                    while scanned < _MAX_SESSION_SCAN:
                        candidates = await session_db.list_sessions_rich(
                            source="telegram",
                            include_children=False,
                            include_archived=False,
                            project_compression_tips=True,
                            order_by_last_active=True,
                            limit=_SESSION_PAGE_SIZE,
                            offset=offset,
                        )
                        page = candidates or []
                        scanned += len(page)
                        for candidate in page:
                            if (
                                str(candidate.get("chat_id") or "") == str(chat_id)
                                and str(candidate.get("thread_id") or "") == str(thread_id)
                                and candidate.get("ended_at") is None
                            ):
                                candidate_id = str(candidate.get("id") or "").strip()
                                if candidate_id and candidate_id not in session_ids:
                                    session_ids.append(candidate_id)
                        if len(page) < _SESSION_PAGE_SIZE:
                            break
                        offset += len(page)
                for session_id in session_ids:
                    await session_db.end_session(session_id, "telegram_topic_closed")
            except Exception:
                logger.warning("Telegram /close session finalization failed", exc_info=True)

        from gateway.becky_loops import source_ref_for

        state = await self._notify_becky_topic_closed(
            source_ref=source_ref_for(chat_id=str(chat_id), thread_id=str(thread_id)),
            closed_at=closed_at,
            control_method=str(getattr(controller, "method", "unavailable")),
        )
        if state == "archived":
            return "Topic closed and archived."
        return "Topic closed. Becky archive is pending; refresh the dashboard shortly."

    async def _notify_becky_topic_closed(
        self, *, source_ref: str, closed_at: datetime, control_method: str
    ) -> str:
        """Notify the local Becky app; never turn a closed topic into a false error."""
        config = getattr(self, "_becky_loops_config", None)
        token = str(getattr(config, "token", "") or "").strip()
        configured_url = os.getenv(
            "HERMES_BECKY_DASHBOARD_URL", "http://127.0.0.1:8787"
        )
        url = _validated_becky_dashboard_url(configured_url)
        if not token or url is None:
            if configured_url and url is None:
                logger.warning(
                    "Ignoring Becky dashboard archive callback with a non-loopback URL"
                )
            return "pending"
        try:
            import httpx

            async with httpx.AsyncClient(
                timeout=5.0,
                follow_redirects=False,
                trust_env=False,
            ) as client:
                response = await client.post(
                    url.rstrip("/") + "/api/internal/telegram-topic-close",
                    headers={"X-Hermes-Loops-Token": token},
                    json={
                        "source_ref": source_ref,
                        "closed_at": closed_at.isoformat(),
                        "control_method": control_method,
                    },
                )
            if response.status_code not in {200, 202}:
                return "pending"
            payload = response.json()
            return "archived" if isinstance(payload, dict) and payload.get("state") == "archived" else "pending"
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("Becky Telegram close archive handoff failed", exc_info=True)
            return "pending"

    async def _start_becky_loops_bridge(self) -> None:
        """Start the opt-in loopback bridge over the gateway's read-only DB."""
        mtproto_controller = None
        try:
            from gateway.becky_loops import (
                TelegramTopicController,
                TelegramTopicSender,
                load_becky_loops_config,
                start_becky_loops_bridge,
                stop_becky_loops_bridge,
            )
            from gateway.telegram_mtproto import MTProtoPrivateTopicController

            config = load_becky_loops_config()
            self._becky_loops_config = config
            db = getattr(self._session_db, "_db", None)
            if config is None or db is None:
                return
            telegram_adapter = self.adapters.get(Platform.TELEGRAM)
            if telegram_adapter is not None:
                setter = getattr(telegram_adapter, "set_becky_close_command_handler", None)
                if callable(setter):
                    setter(self._handle_becky_close_command)
            topic_sender = None
            topic_controller = None
            if config.topic_reply == "bot_api_private_topic":
                if telegram_adapter is not None:
                    topic_sender = TelegramTopicSender(telegram_adapter)
            if config.topic_control == "bot_api_private_topic":
                if telegram_adapter is not None:
                    topic_controller = TelegramTopicController(telegram_adapter)
            elif config.topic_control == "mtproto_private_topic":
                mtproto_controller = MTProtoPrivateTopicController.from_environment(
                    chat_id=config.chat_id
                )
                if mtproto_controller is not None:
                    try:
                        await mtproto_controller.start()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.error(
                            "Becky MTProto topic controller startup failed",
                            exc_info=True,
                        )
                        mtproto_controller = None
                    else:
                        if (
                            bool(getattr(mtproto_controller, "is_connected", False))
                            and bool(getattr(mtproto_controller, "supports_close", False))
                        ):
                            topic_controller = mtproto_controller
                            self._becky_loops_topic_controller = mtproto_controller
                        else:
                            await mtproto_controller.stop()
                            mtproto_controller = None
            if topic_controller is not None:
                self._becky_loops_topic_controller = topic_controller
            bridge = await start_becky_loops_bridge(
                config=config,
                db=db,
                session_store=getattr(self, "session_store", None),
                topic_sender=topic_sender,
                topic_controller=topic_controller,
                agent_dispatcher=(
                    self._dispatch_becky_agent_reply
                    if topic_sender is not None
                    else None
                ),
            )
            startup_should_abort = getattr(self, "_startup_should_abort", None)
            if callable(startup_should_abort) and startup_should_abort():
                if bridge is not None:
                    await stop_becky_loops_bridge(bridge)
                if mtproto_controller is not None:
                    await mtproto_controller.stop()
                self._becky_loops_topic_controller = None
                return
            self._becky_loops_bridge = bridge
            current_adapter = self.adapters.get(Platform.TELEGRAM)
            if bridge is not None and current_adapter is not None and current_adapter is not telegram_adapter:
                self._refresh_becky_telegram_adapter(current_adapter)
            if bridge is None and mtproto_controller is not None:
                await mtproto_controller.stop()
                self._becky_loops_topic_controller = None
            elif bridge is None:
                self._becky_loops_topic_controller = None
        except asyncio.CancelledError:
            if mtproto_controller is not None:
                try:
                    await asyncio.shield(mtproto_controller.stop())
                except BaseException:
                    pass
                self._becky_loops_topic_controller = None
            raise
        except Exception:
            if mtproto_controller is not None:
                try:
                    await mtproto_controller.stop()
                except Exception:
                    logger.debug(
                        "Becky MTProto topic controller cleanup failed",
                        exc_info=True,
                    )
                self._becky_loops_topic_controller = None
            # The bridge is optional; never prevent Telegram or other adapters
            # from starting when its local listener is unavailable.
            logger.error("Becky loop bridge startup failed", exc_info=True)

    async def _stop_becky_loops_bridge(self) -> None:
        """Stop the optional bridge before closing Hermes session state."""
        bridge, self._becky_loops_bridge = (
            getattr(self, "_becky_loops_bridge", None),
            None,
        )
        mtproto_controller, self._becky_loops_topic_controller = (
            getattr(self, "_becky_loops_topic_controller", None),
            None,
        )
        if bridge is not None:
            try:
                from gateway.becky_loops import stop_becky_loops_bridge

                await stop_becky_loops_bridge(bridge)
            except asyncio.CancelledError:
                if mtproto_controller is not None:
                    try:
                        await asyncio.shield(mtproto_controller.stop())
                    except BaseException:
                        pass
                raise
            except Exception:
                logger.debug("Becky loop bridge shutdown failed", exc_info=True)
        if mtproto_controller is not None:
            try:
                await mtproto_controller.stop()
            except Exception:
                logger.debug(
                    "Becky MTProto topic controller shutdown failed",
                    exc_info=True,
                )
