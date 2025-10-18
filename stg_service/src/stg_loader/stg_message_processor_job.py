import time
from datetime import datetime, timezone
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaProducer, KafkaConsumer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository
import json

class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int = 100,
                 logger: Logger = None
                 ) -> None:
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        # Получите сообщение из Kafka
        i: int = 0
        while i < self._batch_size:
            msg = self._consumer.consume()
            if msg is None:
                return
            
            payload = msg['payload']
            payload_json = json.dumps(payload, ensure_ascii=False)
            
            # Сохраните сообщение в таблицу
            self._stg_repository.order_events_insert(
                object_id = msg['object_id'],
                object_type = msg['object_type'],
                sent_dttm = msg['sent_dttm'],
                payload = payload_json
            )            
            # Достаньте id пользователя из сообщения и получите полную информацию о пользователе из Redis.
            user_id = payload['user']['id']
            user = self._redis.get(user_id)
            user_name = user['name']
                       
            # Достаньте id ресторана из сообщения и получите полную информацию о ресторане из Redis.
            restaurant_id = payload['restaurant']['id']
            restaurant = self._redis.get(restaurant_id)
            restaurant_name = restaurant['name']

            menu = restaurant['menu']

            # Сформируйте выходное сообщение.
            products_result = []
            for item in payload['order_items']:
                category = next((menu_item["category"] for menu_item in menu if menu_item["_id"] == item['id']), None)
                product = {
                    "id": item['id'],
                    "price": item['price'],
                    "quantity": item['quantity'],
                    "name": item['name'],
                    "category": category
                }
                products_result.append(product)

            payload_result = {
                "id": msg['object_id'],
                "date": payload["date"],
                "cost": payload["cost"],
                "payment": payload["payment"],
                "status": payload["final_status"],
                "restaurant": {
                    "id": restaurant_id,
                    "name": restaurant_name
                },
                "user": {
                    "id": user_id,
                    "name": user_name
                },
                "products": products_result
            }

            result = {
                "object_id": msg['object_id'],
                "object_type": msg['object_type'],
                "payload": payload_result
            }
            
            # Отправьте выходное сообщение
            self._producer.produce(result)

            i+=1

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.now(datetime.now(timezone.utc))}: FINISH")
