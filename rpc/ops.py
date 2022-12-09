import collections
import random
import threading
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict

import Pyro4

from enums.item_type import Item
from logger import get_logger
from peer.model import Peer
from rpc.rpc_helper import RpcHelper
from utils import get_new_item
from json_files.json_ops import PeerWriter
from collections import deque
LOGGER = get_logger(__name__)


@Pyro4.expose
class CommonOps(ABC):
    def __init__(self,
                 network: Dict[str, Peer],
                 current_peer: Peer,
                 peer_writer: PeerWriter,
                 item_quantities_map: dict,
                 queue: collections.deque,
                 thread_pool_size=30


                 ):
        self._item_quantities_map = item_quantities_map
        self.current_peer = current_peer
        self._pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        self._network = network
        self._neighbours: List[Peer] = self._get_neighbours()
        self._reply_terminated = False
        self.replied_peers: List[Peer] = []
        self._register_product = threading.Lock()
        self._read_write_lock = threading.Lock()
        self._trader_lock = threading.Lock()
        self._warehouse_lock = threading.Lock()
        self._peer_writer = peer_writer
        self._seller_lock = threading.Lock()
        self._change_trader_lock = threading.Lock
        self.queue = queue

    @staticmethod
    def get_product_enum(product):
        return Item(product)

    @staticmethod
    def get_seller_obj(self, seller_id):
        return self._network[seller_id]

    def get_product_price(self, product):
        product = self.get_product_enum(product).value
        quantity, price = self._item_quantities_map[product]
        return price

    def _get_neighbours(self):
        neighbours = self.current_peer.neighbours
        res = []
        for neighbour_id in neighbours:
            neighbour_peer_obj = self._network[neighbour_id]
            res.append(neighbour_peer_obj)
        return res

    def change_trader(self, trader_id):
        #with self._change_trader_lock:
        LOGGER.info("Updating Trader List")
        trader_obj = self._network[trader_id]
        LOGGER.info(f" trader obj : {trader_obj}")
        self.current_peer.trader_list = [(trader_obj.id, trader_obj.host, trader_obj.port)]
        LOGGER.info(f"current obj's trader list :{self.current_peer.trader_list}")

    def buy_on_warehouse(self, buyer_id, product, trader_id):
        with self._warehouse_lock:
            sellers = self._peer_writer.get_sellers()
            LOGGER.info("Inside buy_on_warehouse()")
            LOGGER.info(f"All sellers: {sellers}")

            available_sellers = []
            for seller, seller_val in sellers.items():
                if self._check_if_item_available(product, seller_val['product'], seller_val['quantity']):
                    available_sellers.append(seller)
            LOGGER.info(f"Available Sellers: {available_sellers}")

            if available_sellers:
                selected_seller = random.choice(available_sellers)
                LOGGER.info(f"Selected seller: {selected_seller}")
                sellers[str(selected_seller)]['quantity'] -= 1
                LOGGER.info(f"Quantity remaining: {sellers[str(selected_seller)]['quantity']}")
                LOGGER.info("Buy is successful")

                self._peer_writer.write_sellers(sellers)

                if sellers[str(selected_seller)]['quantity'] == 0:
                    LOGGER.info(
                        f"Item sold! quantity remaining = 0")
                    LOGGER.info(f"Informing the seller to update it's product")
                    LOGGER.info(f"Selected seller agaion: {selected_seller}")
                    seller_obj = self._network[int(selected_seller)]
                    LOGGER.info(f"Seller obj: {seller_obj}")
                    rpn_conn = self._get_rpc_connection(seller_obj.host, seller_obj.port)
                    function_to_execute = lambda: rpn_conn.update_seller_trader(seller_obj.id, product)
                    self._execute_in_thread(function_to_execute)

            self._peer_writer.write_sellers(sellers)
            if not available_sellers:
                LOGGER.info(f"No seller found selling this product!")
                LOGGER.info(f"Informing trader about unavailability of product")
                # raise ValueError("Informing trader about unavailability of product")
                trader_obj = self._network[trader_id]
                rpn_conn = self._get_rpc_connection(trader_obj.host, trader_obj.port)
                function_to_execute = lambda: rpn_conn.buyer_change_item_trader(buyer_id, product)
                self._execute_in_thread(function_to_execute)

    def buyer_change_item_trader(self, buyer_id, product):
        buyer_obj = self._network[buyer_id]
        rpn_conn = self._get_rpc_connection(buyer_obj.host, buyer_obj.port)
        function_to_execute = lambda: rpn_conn.buyer_change_item(buyer_id, product)
        self._execute_in_thread(function_to_execute)

    def buyer_change_item(self, buyer_id, current_item):
        LOGGER.info("No seller found. Please request for another item")
        LOGGER.info(f"Opting new item!")
        new_item = get_new_item(current_item=current_item)
        self.current_peer.item = new_item
        LOGGER.info(f"Buyer {buyer_id} buying new item {new_item}. "
                    f"Old item was {current_item}!")

    def buy_on_trader(self, buyer_id: str, product: Item):
        try:
            if len(self.queue) <= 5:
                with self._trader_lock:
                    product = self.get_product_enum(product).value
                    sellers = self._peer_writer.get_sellers()
                    available_sellers = []
                    for seller, seller_val in sellers.items():
                        if self._check_if_item_available(product, seller_val['product'], seller_val['quantity']):
                            available_sellers.append(seller)
                    LOGGER.info(f"Available Sellers: {available_sellers}")
                    if available_sellers:
                        self.queue.append(buyer_id, product)
                    else:
                        # no seller found
                        # Underselling if the item is present in warehouse
                        LOGGER.info(f"No seller found selling this product!")
                        LOGGER.info(f"Item couldn't be shipped!")
                        LOGGER.info("Please request another item")
                        buyer_obj = self._network[buyer_id]
                        rpn_conn = self._get_rpc_connection(buyer_obj.host, buyer_obj.port)
                        function_to_execute = lambda: rpn_conn.buyer_change_item(buyer_id, product)
                        self._execute_in_thread(function_to_execute)

            else:
                while self.queue:
                    with self._trader_lock:
                        buyer_id,product =  self.queue.popleft()
                    LOGGER.info(
                        f"Buy call by buyer: {buyer_id} on trader: {self.current_peer.id} for product: {product}")
                    # TODO : change the hardcoded value
                    warehouse_obj = self._network[6]
                    LOGGER.info("Calling buy_on_warehouse()")
                    rpc_conn2 = self._get_rpc_connection(warehouse_obj.host, warehouse_obj.port)
                    func_to_execute2 = lambda: rpc_conn2.buy_on_warehouse(buyer_id, product, self.current_peer.id)
                    self._execute_in_thread(func_to_execute2)
        except Exception as e:
            LOGGER.error("ERROR")

    def register_products_warehouse(self, seller_id, product, quantity, trader_id):
        with self._warehouse_lock:
            LOGGER.info(f"Trader : {trader_id} registering seller: {seller_id}'s products on warehouse!")
            sellers = self._peer_writer.get_sellers()
            sellers[seller_id] = {'product': product, 'quantity': quantity}
            LOGGER.info(f"Updated sellers: {sellers}")
            self._peer_writer.write_sellers(sellers)

    def update_seller_trader(self, seller_id, product):
        LOGGER.info("Inside update_seller_")
        LOGGER.info(f"Seller {seller_id} registering its products on Trader!")
        seller_obj = self._network[seller_id]
        rpn_conn = self._get_rpc_connection(seller_obj.host, seller_obj.port)
        function_to_execute = rpn_conn.update_seller(seller_id, product)
        self._execute_in_thread(function_to_execute)

    def update_seller(self, seller_id, product):
        with self._seller_lock:
            # SELLER
            new_item = get_new_item(current_item=product)
            quantity, price = self._item_quantities_map[new_item]

            LOGGER.info(f"New Item Selected: {new_item} with quantity: {quantity} and price: {price}")

            self.current_peer.item = new_item
            self.current_peer.quantity = quantity
            self.current_peer.price = price
            trader = random.Random(self.current_peer.trader_list)
            trader_host = trader[1]
            trader_port = trader[2]
            rpn_conn = self._get_rpc_connection(trader_host, trader_port)
            function_to_execute = lambda: rpn_conn.register_products_trader(seller_id, product, quantity)
            self._execute_in_thread(function_to_execute)

    def register_products_trader(self, seller_id, product, quantity, traderChanged):
        with self._register_product:
            LOGGER.info(f"Inside register_products_trader()")
            # TODO : change the hardcoded value
            warehouse_obj = self._network[6]
            rpn_conn = self._get_rpc_connection(warehouse_obj.host, warehouse_obj.port)
            function_to_execute = lambda: rpn_conn.register_products_warehouse(seller_id, product, quantity,
                                                                               self.current_peer.id)
            self._execute_in_thread(function_to_execute)

    def _execute_in_thread(self, func):
        self._pool.submit(func)

    def get_trader_list(self):
        network_dict = self._peer_writer.get_peers()
        for key in network_dict:
            if network_dict[key]['type'] == "SELLER":
                pass

    @staticmethod
    def _get_rpc_connection(host, port):
        LOGGER.info(f"host {host}, port {port}")
        return RpcHelper(host=host,
                         port=port).get_client_connection()

    @staticmethod
    def _check_if_item_available(product, seller_item, seller_quantity):
        return seller_item == product and seller_quantity > 0

    def shutdown(self):
        self._pool.shutdown()


def update_vector_clock(list1, list2):
    lst = []
    for i in range(len(list1)):
        lst.append(max(list1[i], list2[i]))
    return lst
