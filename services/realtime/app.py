import os, asyncio, json
from fastapi import FastAPI, WebSocket
from aiokafka import AIOKafkaConsumer
from pymongo import MongoClient
from typing import Set

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP","localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC","so-questions")
MONGO_URI = os.environ.get("MONGODB_URI","mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGODB_DB","so_db")
MONGO_COL = os.environ.get("MONGODB_COLLECTION","so_sentiment")

app = FastAPI()
clients: Set[WebSocket] = set()
consumer = None

@app.on_event("startup")
async def startup_event():
    global consumer
    loop = asyncio.get_event_loop()
    consumer = AIOKafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, group_id="realtime-group")
    await consumer.start()
    loop.create_task(kafka_reader())

@app.on_event("shutdown")
async def shutdown_event():
    global consumer
    if consumer:
        await consumer.stop()

async def kafka_reader():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COL]
    async for msg in consumer:
        try:
            payload = json.loads(msg.value.decode("utf-8"))
        except:
            payload = {"raw": msg.value.decode("utf-8")}

        removed = []
        for ws in list(clients):
            try:
                await ws.send_text(json.dumps(payload))
            except:
                try:
                    await ws.close()
                except:
                    pass
                clients.remove(ws)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()  
    except Exception:
        pass
    finally:
        clients.remove(websocket)
