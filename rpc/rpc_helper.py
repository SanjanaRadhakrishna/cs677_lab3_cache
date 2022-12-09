import Pyro4

from logger import get_logger

LOGGER = get_logger(__name__)


class RpcHelper:
    def __init__(self, host, port, scheme="http"):
        self._scheme = scheme + "://"
        self._host = host
        self._port = port
        self._name = "shop"

    def get_client_connection(self):
        uri = f"PYRO:{self._name}@{self._host}:{self._port}"
        return Pyro4.Proxy(uri)

    def start_server(self, ops, id):
        Pyro4.Daemon.serveSimple(
            {
                ops: self._name
            }, host=self._host, port=self._port, ns=False, verbose=False)

        LOGGER.info(f"Started rpc server: {id}!")
