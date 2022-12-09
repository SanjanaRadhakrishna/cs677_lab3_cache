import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer


class RpcHelper:
    def __init__(self, host, port):
        self._host = host
        self._port = port

    def get_client_connection(self):
        return xmlrpc.client.ServerProxy(f"{self._host}:{self._port}/")

    def start_server(self, ops):
        print("Starting rpc server!")
        server = SimpleXMLRPCServer((self._host, self._port), logRequests=True)
        server.register_instance(ops)
        server.serve_forever()
