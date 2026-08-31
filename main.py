import json
import time
import asyncio
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
import clickhouse_connect
from pyexpat.errors import messages

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
ch_client = clickhouse_connect.get_client(host='localhost', port=8123, password='1234')
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
            ch_client.insert('pixel_history', [[x, y, color, user_id]], column_names=['x', 'y', 'color', 'user_id'])
            event = json.dumps({"x": x, "y": y, "color": color, "user": user_id})
            await r.publish("pixel_updates", event)
    except WebSocketDisconnect:
        manager.disconnect(websocket)

