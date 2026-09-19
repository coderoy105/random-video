import asyncio
import json
import logging
import os
import re
from contextlib import suppress

from aiohttp import web
from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    CommentEvent,
    ConnectEvent,
    DisconnectEvent,
    FollowEvent,
    GiftEvent,
    LikeEvent,
    ShareEvent,
)


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("livearcade-events")

clients: set[web.WebSocketResponse] = set()
stream_task: asyncio.Task | None = None
active_username: str | None = None
state_lock = asyncio.Lock()


def clean_username(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._]", "", value.strip().lstrip("@"))


def user_name(event) -> str:
    user = getattr(event, "user", None)
    return clean_username(
        getattr(user, "unique_id", None)
        or getattr(user, "nickname", None)
        or "viewer"
    ) or "viewer"


async def broadcast(payload: dict) -> None:
    if not clients:
        return
    message = json.dumps(payload, ensure_ascii=False)
    closed = []
    for socket in tuple(clients):
        if socket.closed:
            closed.append(socket)
            continue
        try:
            await socket.send_str(message)
        except (ConnectionResetError, RuntimeError):
            closed.append(socket)
    for socket in closed:
        clients.discard(socket)


def gift_payload(event: GiftEvent) -> dict | None:
    gift = getattr(event, "gift", None)
    if gift is None:
        return None

    # Streakable gifts emit intermediate events. The game reacts once, at the
    # end of the streak, to prevent one gift from dealing repeated damage.
    if getattr(event, "streaking", False):
        return None

    gift_name = str(getattr(gift, "name", None) or "Gift")
    repeat_count = int(getattr(event, "repeat_count", 1) or 1)
    gift_type = int(getattr(gift, "type", 0) or 0)
    diamond_count = int(getattr(gift, "diamond_count", 0) or 0)
    gift_value = getattr(event, "value", None)
    return {
        "type": "gift",
        "giftName": gift_name,
        "giftType": gift_type,
        "repeatCount": repeat_count,
        "diamondCount": diamond_count,
        "giftValue": round(float(gift_value), 4) if gift_value is not None else None,
        "nickname": user_name(event),
        "timestamp": asyncio.get_running_loop().time(),
    }


async def run_tiktok(username: str) -> None:
    global active_username
    first_attempt = True
    while active_username == username:
        client = TikTokLiveClient(unique_id=username)

        @client.on(ConnectEvent)
        async def on_connect(event: ConnectEvent):
            logger.info("Connected to @%s", username)
            await broadcast({"type": "server", "status": "connected", "tiktokId": username})

        @client.on(CommentEvent)
        async def on_comment(event: CommentEvent):
            await broadcast({
                "type": "comment",
                "comment": str(getattr(event, "comment", "")),
                "nickname": user_name(event),
            })

        @client.on(GiftEvent)
        async def on_gift(event: GiftEvent):
            payload = gift_payload(event)
            if payload:
                await broadcast(payload)

        @client.on(LikeEvent)
        async def on_like(event: LikeEvent):
            await broadcast({
                "type": "like",
                "likeCount": int(getattr(event, "like_count", 1) or 1),
                "nickname": user_name(event),
            })

        @client.on(FollowEvent)
        async def on_follow(event: FollowEvent):
            await broadcast({"type": "follow", "nickname": user_name(event)})

        @client.on(ShareEvent)
        async def on_share(event: ShareEvent):
            await broadcast({"type": "share", "nickname": user_name(event)})

        @client.on(DisconnectEvent)
        async def on_disconnect(event: DisconnectEvent):
            await broadcast({"type": "server", "status": "disconnected", "tiktokId": username})

        try:
            await client.connect(fetch_gift_info=True)
        except asyncio.CancelledError:
            with suppress(Exception):
                await client.disconnect()
            raise
        except Exception as error:
            logger.warning("TikTok connection failed for @%s: %s", username, error)
            await broadcast({
                "type": "server",
                "status": "error" if first_attempt else "waiting",
                "tiktokId": username,
                "message": str(error)[:240],
            })
        finally:
            with suppress(Exception):
                await client.disconnect()

        first_attempt = False
        if active_username != username:
            break
        await broadcast({
            "type": "server",
            "status": "waiting",
            "tiktokId": username,
            "message": "LIVE 연결을 다시 시도하는 중입니다.",
        })
        await asyncio.sleep(15)

    if active_username == username:
        active_username = None


async def switch_stream(username: str) -> None:
    global stream_task, active_username
    async with state_lock:
        if active_username == username and stream_task and not stream_task.done():
            return
        if stream_task and not stream_task.done():
            stream_task.cancel()
            with suppress(asyncio.CancelledError):
                await stream_task
        active_username = username
        stream_task = asyncio.create_task(run_tiktok(username))


async def stop_stream() -> None:
    global stream_task, active_username
    async with state_lock:
        if stream_task and not stream_task.done():
            stream_task.cancel()
            with suppress(asyncio.CancelledError):
                await stream_task
        stream_task = None
        active_username = None


async def health(request: web.Request) -> web.Response:
    return web.json_response({
        "ok": True,
        "service": "livearcade-tiktok-events",
        "tiktokId": active_username,
        "clients": len(clients),
    })


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    socket = web.WebSocketResponse(heartbeat=25)
    await socket.prepare(request)
    clients.add(socket)
    await socket.send_json({
        "type": "server",
        "status": "ready",
        "tiktokId": active_username,
    })

    try:
        async for message in socket:
            if message.type != web.WSMsgType.TEXT:
                continue
            try:
                payload = json.loads(message.data)
            except json.JSONDecodeError:
                await socket.send_json({"type": "server", "status": "error", "message": "Invalid JSON"})
                continue

            if payload.get("type") != "subscribe":
                continue
            username = clean_username(str(payload.get("tiktokId", "")))
            if not username:
                await socket.send_json({"type": "server", "status": "error", "message": "TikTok ID is required"})
                continue
            await socket.send_json({"type": "server", "status": "connecting", "tiktokId": username})
            await switch_stream(username)
    finally:
        clients.discard(socket)
        if not clients:
            await stop_stream()
    return socket


async def on_shutdown(app: web.Application) -> None:
    if stream_task and not stream_task.done():
        stream_task.cancel()
        with suppress(asyncio.CancelledError):
            await stream_task


app = web.Application()
app.router.add_get("/health", health)
app.router.add_get("/ws", websocket_handler)
app.on_shutdown.append(on_shutdown)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    web.run_app(app, host="0.0.0.0", port=port)
