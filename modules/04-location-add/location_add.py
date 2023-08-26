import json
import os

from kafka import KafkaConsumer

TOPIC_NAME = os.environ["KAFKA_TOPIC"]

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

KAFKA_URL = os.environ["KAFKA_URL"]

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[KAFKA_URL])
print('listening topic ' + TOPIC_NAME)

def save_in_db(location):
    from sqlalchemy import create_engine

    engine = create_engine(f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", echo=True)
    conn = engine.connect()

    person_id = int(location["person_id"])
    print('person_id: ' + person_id)
    latitude, longitude = int(location["latitude"]), int(location["longitude"])

    insert = "INSERT INTO location (person_id, coordinate) VALUES ({}, ST_Point({}, {}))" \
        .format(person_id, latitude, longitude)

    print('sql insert: ' + insert)
    conn.execute(insert)


for location in consumer:
    message = location.value.decode('utf-8')
    print('{}'.format(message))
    location_message = json.loads(message)
    save_in_db(location_message)
