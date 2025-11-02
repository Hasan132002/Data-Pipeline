import json
import time
from kafka import KafkaProducer
from pathlib import Path
from scripts.utils import logger

RAW_JSON = Path(r"D:\BDA\projects\so-pipeline\stackoverflow_questions.json")
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "so-questions"

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100
    )
    logger.info("Starting to stream lines to Kafka topic 'so-questions'")
    with open(RAW_JSON, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            producer.send(TOPIC, payload)
            if i % 1000 == 0:
                producer.flush()
                logger.info(f"Produced {i} messages")
                time.sleep(0.1)
    producer.flush()
    logger.info("Finished producing all messages")

if __name__ == "__main__":
    main()
