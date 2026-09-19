# LiveArcade TikTok LIVE Event Server

This service connects to a public TikTok LIVE using the broadcaster's unique ID and broadcasts normalized events to browser clients over WebSocket.

## Endpoints

- `GET /health`
- `WS /ws`

After opening `/ws`, send:

```json
{"type":"subscribe","tiktokId":"creator_id"}
```

The server emits `gift`, `like`, `comment`, `follow`, and `share` events. The LiveArcade browser maps these events into game actions.

## Important

This uses the unofficial TikTokLive reverse-engineering client. TikTok can change the Webcast protocol or rate-limit the connection. Do not place TikTok passwords, cookies, or Gift payment data in this service.
