# ./src/microservices/events/main.py

import os
import json
import logging
import threading
from time import sleep
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация Kafka ---
# Читаем переменную KAFKA_BROKERS из docker-compose.yml
KAFKA_BROKER = os.getenv('KAFKA_BROKERS', 'kafka:9092')
USER_TOPIC = "user-events"
PAYMENT_TOPIC = "payment-events"
MOVIE_TOPIC = "movie-events"
ALL_TOPICS = [USER_TOPIC, PAYMENT_TOPIC, MOVIE_TOPIC]
MAX_RETRIES = 5
RETRY_DELAY = 5

# --- Модели данных ---
class EventPayload(BaseModel):
    # Тесты отправляют тело запроса без вложенности, например {"user_id": "123"}
    # Поэтому мы не используем 'payload' и принимаем любой словарь
    pass  # Позволяет FastAPI принимать любой валидный JSON как Dict

# --- Инициализация FastAPI ---
app = FastAPI(
    title="Events Service",
    description="Сервис для отправки и чтения событий Kafka согласно тестам",
    version="1.2.0" # Обновим версию для ясности
)

# --- Логика Kafka Producer ---
def get_kafka_producer():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(2, 6, 0) # Явно укажем версию API для совместимости
            )
            logging.info("Kafka Producer успешно подключен.")
            return producer
        except NoBrokersAvailable:
            retries += 1
            logging.warning(f"Producer: Не удалось подключиться к Kafka. Попытка {retries}/{MAX_RETRIES}...")
            sleep(RETRY_DELAY)
    logging.error("Не удалось подключиться к Kafka Producer после нескольких попыток.")
    return None

producer = get_kafka_producer()

# --- Логика Kafka Consumer ---
def consume_events():
    logging.info("Запуск Kafka Consumer в фоновом потоке...")
    retries = 0
    consumer = None
    while retries < MAX_RETRIES and consumer is None:
        try:
            consumer = KafkaConsumer(
                *ALL_TOPICS,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                group_id='events-consumer-group-final',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                api_version=(2, 6, 0)
            )
            logging.info("Kafka Consumer успешно подключен.")
        except Exception as e:
            retries += 1
            logging.warning(f"Consumer: Не удалось подключиться к Kafka ({e}). Попытка {retries}/{MAX_RETRIES}...")
            sleep(RETRY_DELAY)

    if not consumer:
        logging.error("Consumer: Не удалось подключиться к Kafka. Поток завершает работу.")
        return

    logging.info(f"Consumer слушает топики: {ALL_TOPICS}...")
    for message in consumer:
        logging.info(
            f"CONSUMED EVENT: "
            f"Topic: {message.topic}, "
            f"Value: {message.value}"
        )

# --- Запуск консьюмера при старте приложения ---
@app.on_event("startup")
async def startup_event():
    if producer is None:
        logging.error("Приложение не может запуститься без Kafka Producer.")
        return
    consumer_thread = threading.Thread(target=consume_events, daemon=True)
    consumer_thread.start()

# --- API Endpoints (согласно тестам) ---

@app.get("/api/events/health")
async def health_check():
    return {"status": True}

@app.post("/api/events/user", status_code=status.HTTP_201_CREATED)
async def create_user_event(data: Dict[str, Any]):
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka Producer не доступен")
    logging.info(f"PRODUCING to '{USER_TOPIC}': {data}")
    producer.send(USER_TOPIC, value=data)
    producer.flush()
    return {"status": "success"}

@app.post("/api/events/payment", status_code=status.HTTP_201_CREATED)
async def create_payment_event(data: Dict[str, Any]):
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka Producer не доступен")
    logging.info(f"PRODUCING to '{PAYMENT_TOPIC}': {data}")
    producer.send(PAYMENT_TOPIC, value=data)
    producer.flush()
    return {"status": "success"}

@app.post("/api/events/movie", status_code=status.HTTP_201_CREATED)
async def create_movie_event(data: Dict[str, Any]):
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka Producer не доступен")
    logging.info(f"PRODUCING to '{MOVIE_TOPIC}': {data}")
    producer.send(MOVIE_TOPIC, value=data)
    producer.flush()
    return {"status": "success"}
