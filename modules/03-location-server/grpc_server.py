from concurrent import futures
from kafka import KafkaProducer
import grpc
import location_pb2
import location_pb2_grpc
import json
import os


TOPIC_NAME = os.environ["KAFKA_TOPIC"]
KAFKA_SERVER = os.environ["KAFKA_URL"]


producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

print('connecting to kafka ', KAFKA_SERVER)
print('connecting to kafka topic ', TOPIC_NAME)

class LocationServicer(location_pb2_grpc.LocationServiceServicer):
    def CreateLocation(self, request, context):
        request_value = {
            'person_id': request.person_id,
            'longitude': request.longitude,
            'latitude': request.latitude
        }
        topic_message = json.dumps(request_value).encode('utf-8')
        producer.send(TOPIC_NAME, topic_message)
        producer.flush()
        return location_pb2.LocationMessage(**request_value)

server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
location_pb2_grpc.add_LocationServiceServicer_to_server(LocationServicer(), server)

server.add_insecure_port("[::]:5005")
server.start()

server.wait_for_termination()