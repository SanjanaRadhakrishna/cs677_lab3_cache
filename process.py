import collections
import multiprocessing
import random
import threading
from threading import Thread
from time import sleep
from typing import Dict
import Pyro4
import config
import json_files.json_ops
from config import params_pickle_file_path
from enums.peer_type import PeerType
from logger import get_logger
from peer.model import Peer
from rpc.ops_factory import OpsFactory
from rpc.rpc_helper import RpcHelper
from utils import pickle_load_from_file, get_new_item, get_free_port
from concurrent.futures import ThreadPoolExecutor

LOGGER = get_logger(__name__)

data_lock = threading.Lock()

pool = ThreadPoolExecutor(max_workers=20)
leader_lock = threading.Lock()

peers_lock = multiprocessing.Lock()
sellers_lock = multiprocessing.Lock()

queue = collections.dequeue()


def heartbeat(trader_obj, current_peer_obj, network_map):
    LOGGER.info(f"Checking liveliness of the Trader :{trader_obj.id}")
    uri = f"PYRO:shop@{trader_obj.host}:{trader_obj.port}"
    with Pyro4.Proxy(uri) as p:
        try:
            p._pyroBind()
            LOGGER.info(f"Trader {trader_obj.id} is OK")
        except:
            LOGGER.info("No reply from other trader")
            # Inform all peers that only one trader left
            LOGGER.info(f" I am the sole trader : {current_peer_obj.id} ")
            pool = ThreadPoolExecutor(max_workers=20)

            network_map.pop(trader_obj.id, None)
            LOGGER.info(f"network map: {network_map.keys()}")
            current_peer_obj.trader_list = [(current_peer_obj.id, current_peer_obj.host, current_peer_obj.port)]
            for peer_id, peer_obj in network_map.items():
                if peer_id == trader_obj:
                    continue
                LOGGER.info(f"Update {peer_id}'s trader list")
                try:
                    helper = RpcHelper(host=peer_obj.host, port=peer_obj.port)
                    peer_conn = helper.get_client_connection()
                    peer_conn.change_trader(current_peer_obj.id)
                except Exception as e:
                    LOGGER.error(e)


def handle_process_start(ops, current_peer_obj: Peer, network_map: Dict[str, Peer], peer_writer):
    current_id = current_peer_obj.id
    LOGGER.info(f"Start process called for peer {current_id}. Sleeping!")
    sleep(10)

    if current_peer_obj.type == PeerType.BUYER:
        LOGGER.info(f"Waiting for sellers to register their products with the trader...Sleeping!")
        sleep(10)
        LOGGER.info(f"Initializing buyer flow for peer {current_id}")
        while True:
            current_item = current_peer_obj.item
            LOGGER.info(f"Buyer {current_id} sending buy request for item {current_item}")
            try:
                trader = random.choice(current_peer_obj.trader_list)
                trader_id = trader[0]
                trader_host = trader[1]
                trader_port = trader[2]
                LOGGER.info(f" Selected trader : {trader_id} trader host {trader_host} , trader port {trader_port}")
                LOGGER.info(f" Buyer is requesting the item from the trader : {trader_id}")
                helper = RpcHelper(host=trader_host, port=trader_port)
                trader_connection = helper.get_client_connection()
                trader_connection.buy_on_trader(current_id, current_item)
                LOGGER.info(f"Transaction is complete")
                sleep(10)

            except ValueError as e:
                LOGGER.info("No seller found. Please request for another item")
                LOGGER.info(f"Opting new item!")
                new_item = get_new_item(current_item=current_item)
                LOGGER.info(f"Buyer {current_id} buying new item {new_item}. "
                            f"Old item was {current_item}!")
                current_peer_obj.item = new_item
            except Exception as ex:
                LOGGER.exception(f"Failed to execute buy call")

    if current_peer_obj.type == PeerType.SELLER:
        trader = random.choice(current_peer_obj.trader_list)
        trader_id = trader[0]
        trader_host = trader[1]
        trader_port = trader[2]
        LOGGER.info(f"Seller: {current_peer_obj.id} is registering items with the trader: {trader_id}")
        helper = RpcHelper(host=trader_host, port=trader_port)
        trader_connection = helper.get_client_connection()
        trader_connection.register_products_trader(current_peer_obj.id, current_peer_obj.item,
                                                   current_peer_obj.quantity)
        LOGGER.info(f" Seller successfully registered its items")

    if current_peer_obj.type == PeerType.TRADER:
        trader_list = current_peer_obj.trader_list
        for trader in trader_list:
            if current_peer_obj.id == trader[0]:
                continue
            trader_obj = network_map[trader[0]]
            while True:
                trader_list = current_peer_obj.trader_list
                if len(trader_list) <= 1:
                    break
                heartbeat(trader_obj, current_peer_obj, network_map)
                sleep(2)


def start_process(current_peer_id: int):
    peer_writer = json_files.json_ops.PeerWriter(peers_lock=peers_lock, sellers_lock=sellers_lock)
    data = pickle_load_from_file(params_pickle_file_path)

    network_map = data["network_map"]
    item_quantities_map = config.item_quantities_map
    thread_pool_size = config.thread_pool_size

    current_peer_obj: Peer = network_map[current_peer_id]

    ops_obj = OpsFactory.get_ops(network=network_map,
                                 current_peer=current_peer_obj,
                                 item_quantities_map=item_quantities_map,
                                 thread_pool_size=thread_pool_size,
                                 peer_writer=peer_writer,
                                 queue= queue)

    trigger_thread = Thread(target=handle_process_start, args=(ops_obj, current_peer_obj, network_map, peer_writer))
    trigger_thread.start()

    helper = RpcHelper(host=current_peer_obj.host, port=current_peer_obj.port)

    helper.start_server(ops_obj, current_peer_obj.id)

    LOGGER.info(f"Done with processing :{current_peer_obj}")
    ops_obj.shutdown()
