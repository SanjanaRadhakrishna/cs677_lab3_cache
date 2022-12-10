import argparse
import multiprocessing
from multiprocessing import Pool, Process
from threading import Thread

import config
import json_files.json_ops
from config import item_quantities_map, params_pickle_file_path
from enums.peer_type import peer_types, PeerType
from logger import get_logger
from peer.network import NetworkCreator
from peer.peer_generator import PeerGenerator
from process import start_process, handle_process_start
from rpc.ops_factory import OpsFactory
from rpc.rpc_helper import RpcHelper
from utils import pickle_to_file

LOGGER = get_logger(__name__)
from process import sellers_lock, peers_lock
from json_files.json_ops import PeerWriter

peer_writer = PeerWriter(peers_lock, sellers_lock)


def create_and_get_network(num_peers: int) -> dict:
    peer_generator = PeerGenerator(num_peers=num_peers,
                                   peer_types=peer_types,
                                   item_quantities_map=item_quantities_map)
    peers = peer_generator.init_and_get_peers()

    network_generator = NetworkCreator(nodes=peers)
    network = network_generator.generate_network()

    network_dict = {}
    for peer_id, peer in network.items():
        new_dict = peer.__dict__
        network_dict[peer_id] = new_dict
    peer_writer.write_peers(network_dict)

    leader_1, leader_2 = initial_leader_election()
    network[leader_1].type = PeerType.TRADER
    network[leader_2].type = PeerType.TRADER

    for peer_id, peer in network.items():
        network[peer_id].trader_list = [(leader_1, network[leader_1].host, network[leader_1].port),
                                        (leader_2, network[leader_2].host, network[leader_2].port)]

    sellers = {}
    filename = 'warehouse.json'
    with open(filename, 'w') as f:
        f.write('')
    peer_writer.write_sellers(sellers, 'warehouse')

    filename = 'trader_cache_' + str(leader_1)+'.json'
    with open(filename, 'w') as f:
        f.write('')
    filename = 'trader_cache_' + str(leader_1)
    sellers = peer_writer.write_sellers(sellers, filename)

    filename = 'trader_cache_' + str(leader_2)+'.json'
    with open(filename, 'w') as f:
        f.write('')
    filename = 'trader_cache_' + str(leader_2)
    sellers = peer_writer.write_sellers(sellers, filename)

    LOGGER.info("------------Network------------")
    network_generator.print(network)
    return network

def initial_leader_election():
    LOGGER.info("Initial Leader Election")
    network_dict = peer_writer.get_peers()
    leader = 0
    for peer_id, peer_dict in network_dict.items():
        if int(peer_id) > leader:
            leader = int(peer_id)

    leader_1 = leader - 1
    leader_2 = leader - 2
    for peer_id, peer_dict in network_dict.items():
        network_dict[peer_id]['trader_list'] = [
            (leader_1, network_dict[str(leader_1)]['host'], network_dict[str(leader_1)]['port']),
            (leader_2, network_dict[str(leader_2)]['host'], network_dict[str(leader_2)]['port'])]

    network_dict[str(leader_1)]['type'] = "TRADER"
    network_dict[str(leader_2)]['type'] = "TRADER"

    peer_writer.write_peers(network_dict)
    LOGGER.info(f"Leaders elected are: {str(leader_1), str(leader_2)}")
    return [leader_1, leader_2]


def spawn_child_processes(network_map: dict, num_peers: int):
    peer_ids = list(range(num_peers))

    process_params = {
        "network_map": network_map,
    }
    pickle_to_file(file_path=params_pickle_file_path, data=process_params)

    with Pool(num_peers) as p:
        p.map(start_process, peer_ids)


def initialize_app(num_peers: int):
    # Create the network map
    network = create_and_get_network(num_peers)

    # # Create a warehouse
    # create_warehouse(network)

    # Spawn child processes
    spawn_child_processes(network, num_peers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-N', '--num_peers', type=int,
                        required=True,
                        help='Number of peer processes')

    args = parser.parse_args()
    num_peers = args.num_peers
    LOGGER.info(f"Number of peers required is {num_peers}")
    # Initialize the application
    initialize_app(num_peers)
