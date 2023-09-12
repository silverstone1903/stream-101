import json
from kafka import KafkaProducer
import time
from faker import Faker
import random
import cfg

producer = KafkaProducer(
    bootstrap_servers=cfg.config.bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

fake = Faker()

while True:
    data = {
        "name": fake.name(),
        "mail": fake.email(),
        "job": fake.job(),
        "addres": fake.address(),
        "country": fake.country(),
        "age": random.randint(18, 80),
        "salary": random.randint(1000, 10000),
        "register_date": fake.date(),
    }
    producer.send(cfg.config.topic, value=data)
    time.sleep(random.randint(0, 10))
