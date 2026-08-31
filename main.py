import json
import time
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as redis
from pyexpat.errors import messages

app = FastAPI()

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

WIDTH = 100
HEIGHT = 100
COOLDOWN = 30

@app.get("/")
async def root():
    return {"message": "Сервер запущен, стучись в /ws через вебсокеты"}

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(redis_listener())


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)
manager = ConnectionManager()

async def redis_listener():
    pubsub = r.pubsub()
    await pubsub.subscribe("pixel_updates")
    async for message in pubsub.listen():
        if message["type"] == "message":
            await manager.broadcast(message["data"])

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            user_id = payload.get("user_id") or payload.get("user")
            x = payload.get("x")
            y = payload.get("y")
            color = payload.get("color")
            if not user_id:
                continue
            if await r.exists(f"user:{user_id}:cd"):
                await websocket.send_text(json.dumps({"error":"Кулдаун еще не прошел"},ensure_ascii=False))
                continue
            await r.set(f"user:{user_id}:cd", "1", ex=COOLDOWN)
            pixel_index = y * WIDTH + x
            await r.hset("canvas", pixel_index, color)
            event = json.dumps({"x": x, "y": y, "color": color, "user": user_id})
            await r.publish("pixel_updates", event)
    except WebSocketDisconnect:
        manager.disconnect(websocket)

