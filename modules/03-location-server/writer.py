import grpc
import location_pb2
import location_pb2_grpc

"""
Sample implementation of a writer that can be used to write messages to gRPC.
"""

print("gRPC sample is sending...")

channel = grpc.insecure_channel("localhost:5005")
location_stub = location_pb2_grpc.LocationServiceStub(channel)

# Update this with desired payload
locations = location_pb2.LocationsMessage(
    person_id=100,
    longitude= 1111.11111,
    latitude= 2222.22222
)

response = location_stub.CreateLocation(locations)
print("responding...")
print(response)