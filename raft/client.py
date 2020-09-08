import grpc
import sys

from core.server.service_pb2_grpc import RaftServiceStub
from core.server.service_pb2 import ClientWriteRequest

channel = grpc.insecure_channel(sys.argv[1])
stub = RaftServiceStub(channel)
response = stub.ClientWrite(ClientWriteRequest(key=sys.argv[2], value=sys.argv[3]))
print("Raft Service client received: {}".format(response.success))