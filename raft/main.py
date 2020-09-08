# refer to https://github.com/grpc/grpc/blob/v1.31.0/examples/python/helloworld/greeter_server.py

from concurrent import futures
import socket
import logging
import sys

import grpc
from core.server import service_pb2_grpc
from core.server.server import RPCServer
from core.states import Leader, Follower, Candidate


def serve():
    print(sys.argv)
    addr = sys.argv[1]
    peers = sys.argv[2].split(',') if len(sys.argv) >= 3 else []
    rpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_RaftServiceServicer_to_server(RPCServer(addr, peers), rpc_server)
    rpc_server.add_insecure_port('[::]:{}'.format(addr.split(":")[1]))
    rpc_server.start()
    rpc_server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
