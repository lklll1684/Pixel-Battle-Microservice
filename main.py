import json
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as redis
from pyexpat.errors import messages

app = FastAPI()

r = redis.Redis(host='localhost', port=6379, db=, decode_responses=True)

WIDTH = 100
HEIGHT = 100
COLLDOWN = 30

@app.on_event("startup")
async def startup():
    print("Сервер запущен")


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
            user_id = payload.get["user_id"]
            x = payload.get["x"]
            y = payload.get["y"]
            color = payload.get["color"]
            last_time = await r.get(f"user: {user_id}:cd")
            current_time = time.time()
            if last_time and (current_time - float(last_time)) > COLLDOWN:
                await websocket.send_text(json.dumps({"error": "Кулдаун еще не прошел"}))
                continue

            await r.set(f"user: {user_id}:cd", current_time)
            pixel_index = y * WIDTH + x
            await r.hset("canvas", pixel_index, color)
            event = json.dumps({"x": x, "y": y, "color": color, "user": user_id})
            await r.publish("pixel_updates", event)
    except WebSocketDisconnect:
        manager.disconnect(websocket)

