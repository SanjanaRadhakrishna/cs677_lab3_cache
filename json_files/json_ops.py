import json
from random import random
from time import sleep
from typing import Dict, List

# Log a transaction
from logger import get_logger
from peer.model import Peer

LOGGER = get_logger(__name__)


class PeerWriter:
    def __init__(self, peers_lock, sellers_lock
                 ):
        self.peers_lock = peers_lock
        self.sellers_lock = sellers_lock

    def get_lock(self):
        return self.method_lock

    def write_peers(self, network: Dict[str, Peer]):
        LOGGER.info(f"Writing network to file")
        sleep(random())
        with open('peers.json', 'w') as f:
            json.dump(network, f)
        LOGGER.info("Writing to a file is successful")

    def get_peers(self):
        LOGGER.info("Reading from a file")
        sleep(random())
        with open('peers.json', 'r') as file:
            data = json.load(file)
        LOGGER.info("Reading from a file is successful")
        return data

    def get_sellers(self,file_name):
        LOGGER.info(f"Reading seller from a {file_name} file")
        file_name = file_name+'.json'
        with open(file_name,'r') as f:
            data = json.load(f)
        LOGGER.info(f"Reading from {file_name} file is successful")
        return data

    def write_sellers(self,sellers,file_name):
        LOGGER.info(f"Writing sellers to {file_name} file")
        sleep(random())
        file_name = file_name + '.json'
        with open(file_name, 'w') as f:
            json.dump(sellers, f)
        LOGGER.info(f"Writing to a {file_name} file is successful")




