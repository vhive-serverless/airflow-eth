import argparse
import importlib
import logging
import grpc
from datetime import datetime
from concurrent import futures

from h2c_pb2_grpc import add_H2CServicer_to_server

logging.basicConfig(level=logging.INFO)


def serve(port, func):
    logging.info(f'Starting gRPC server on port[{port}]')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    server.add_insecure_port(f'[::]:{port}')
    add_H2CServicer_to_server(func, server)

    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=8080)
    parser.add_argument('--handler', type=str, default="dummy_task:dummy_ti_connector")
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    logging.info(f"[Server][{datetime.now()}] Starting...")
    args = main()
    logging.info(f"[Server][{datetime.now()}] Got arguments...")

    file_name, handler_name = args.handler.split(":")
    lib = importlib.import_module(file_name)
    func = lib.__getattribute__(handler_name)
    logging.info(f"[Server][{datetime.now()}] Got dummy task instance '{func}'...")

    logging.info(f"[Server][{datetime.now()}] Open server...")
    serve(8080, func)
