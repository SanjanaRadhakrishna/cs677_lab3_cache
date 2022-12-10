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

LOGGER = get_logger(__name__)


@Pyro4.expose
class CommonOps(ABC):
    def __init__(self,
                 network: Dict[str, Peer],
                 current_peer: Peer,
                 peer_writer: PeerWriter,
                 item_quantities_map: dict,
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
        self.num_peers = len(network)

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
        LOGGER.info("Updating Trader List")
        trader_obj = self._network[trader_id]
        self.current_peer.trader_list = [(trader_obj.id, trader_obj.host, trader_obj.port)]
        LOGGER.info(f"Current peer: {self.current_peer.id} trader list :{self.current_peer.trader_list}")

    def update_cache(self, db_cache):
        trader_id = self.current_peer.id
        filename = 'trader_cache_' + str(trader_id)
        self._peer_writer.write_sellers(db_cache, filename)
        LOGGER.info(f"Updated trader: {trader_id}'s cache after 10s")

    def buy_on_warehouse(self, buyer_id, product, trader_id):
        with self._warehouse_lock:
            file_name = 'warehouse'
            sellers = self._peer_writer.get_sellers(file_name)
            available_sellers = []
            for seller, seller_val in sellers.items():
                if self._check_if_item_available(product, seller_val['product'], seller_val['quantity']):
                    available_sellers.append(seller)
            LOGGER.info(f"Available Sellers in warehouse cache: {available_sellers}")

            if available_sellers:
                selected_seller = random.choice(available_sellers)
                LOGGER.info(f"Selected seller on warehouse: {selected_seller}")
                sellers[str(selected_seller)]['quantity'] -= 1
                LOGGER.info(f"Quantity remaining: {sellers[str(selected_seller)]['quantity']}")
                LOGGER.info(f"Buy is successful. Buyer:{buyer_id} item: {product} got shipped!")
                self._peer_writer.write_sellers(sellers, file_name)

                if sellers[str(selected_seller)]['quantity'] == 0:
                    LOGGER.info(
                        f"Item sold! quantity remaining = 0")
                    LOGGER.info(f"Informing the seller to update it's product")
                    seller_obj = self._network[int(selected_seller)]
                    LOGGER.info(f"Seller obj: {seller_obj}")
                    rpn_conn = self._get_rpc_connection(seller_obj.host, seller_obj.port)
                    function_to_execute = lambda: rpn_conn.update_seller_trader(seller_obj.id, product)
                    self._execute_in_thread(function_to_execute)

            self._peer_writer.write_sellers(sellers, file_name=file_name)
            if not available_sellers:
                LOGGER.info(f"No seller found selling this product!")
                LOGGER.info(f"Informing trader: {trader_id} about unavailability of product")
                LOGGER.info("Item couldn't be shipped!")
                LOGGER.info(f"OVERSELLING: Requested buy transaction couldn't be completed for buyer :{buyer_id}")
                trader_obj = self._network[trader_id]
                rpn_conn = self._get_rpc_connection(trader_obj.host, trader_obj.port)
                function_to_execute = lambda: rpn_conn.buyer_change_item_trader(buyer_id, product)
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
        with self._trader_lock:
            try:
                product = self.get_product_enum(product).value
                trader_id = self.current_peer.id
                filename = 'trader_cache_' + str(trader_id)
                sellers = self._peer_writer.get_sellers(filename)
                available_sellers = []
                for seller, seller_val in sellers.items():
                    if self._check_if_item_available(product, seller_val['product'], seller_val['quantity']):
                        available_sellers.append(seller)
                LOGGER.info(f"Available Sellers in trader's cache: {available_sellers}")
                if available_sellers:
                    LOGGER.info(
                        f"Trader: {trader_id} is selling product: {product} to buyer:{buyer_id}")
                    warehouse_obj = self._network[self.num_peers-1]
                    rpc_conn2 = self._get_rpc_connection(warehouse_obj.host, warehouse_obj.port)
                    func_to_execute2 = lambda: rpc_conn2.buy_on_warehouse(buyer_id, product, self.current_peer.id)
                    self._execute_in_thread(func_to_execute2)

                else:
                    # no seller found
                    # Underselling if the item is present in warehouse
                    LOGGER.info(f"No seller found selling this product!")
                    LOGGER.info(f"Item couldn't be shipped!")
                    buyer_obj = self._network[buyer_id]
                    rpn_conn = self._get_rpc_connection(buyer_obj.host, buyer_obj.port)
                    function_to_execute = lambda: rpn_conn.buyer_change_item(buyer_id, product)
                    self._execute_in_thread(function_to_execute)

            except Exception as e:
                LOGGER.error("ERROR")
                LOGGER.error(e)

    def register_products_warehouse(self, seller_id, product, quantity, trader_id):
        with self._warehouse_lock:
            LOGGER.info(f"Trader : {trader_id} registering seller: {seller_id}'s products on warehouse!")
            ware_house = self._peer_writer.get_sellers('warehouse')
            if ware_house:
                if str(seller_id) in ware_house:
                    ware_house[str(seller_id)]['quantity'] = quantity
                    ware_house[str(seller_id)]['product'] = product
                else:
                    ware_house[str(seller_id)] = {'product': product, 'quantity': quantity}
            else:
                ware_house = {str(seller_id): {'product': product, 'quantity': quantity}}

            LOGGER.info(f"Ware house sellers: {ware_house}")
            self._peer_writer.write_sellers(ware_house, 'warehouse')

            trader_id = 4
            filename = 'trader_cache_' + str(trader_id)
            sellers = self._peer_writer.get_sellers(filename)

            if sellers:
                if str(seller_id) in sellers:
                    sellers[str(seller_id)]['quantity'] = quantity
                    sellers[str(seller_id)]['product'] = product
                else:
                    sellers[str(seller_id)] = {'product': product, 'quantity': quantity}
            else:
                sellers = {str(seller_id): {'product': product, 'quantity': quantity}}
            self._peer_writer.write_sellers(sellers, filename)

            trader_id = 5
            filename = 'trader_cache_' + str(trader_id)
            sellers = self._peer_writer.get_sellers(filename)

            if sellers:
                if str(seller_id) in sellers:
                    sellers[str(seller_id)]['quantity'] = quantity
                    sellers[str(seller_id)]['product'] = product
                else:
                    sellers[str(seller_id)] = {'product': product, 'quantity': quantity}
            else:
                sellers = {str(seller_id): {'product': product, 'quantity': quantity}}
            self._peer_writer.write_sellers(sellers, file_name=filename)

    def update_seller_trader(self, seller_id, product, ng):
        LOGGER.info(f"Seller {seller_id} registering its products on Trader!")
        seller_obj = self._network[seller_id]
        rpn_conn = self._get_rpc_connection(seller_obj.host, seller_obj.port)
        function_to_execute = rpn_conn.update_seller(seller_id, product, ng)
        self._execute_in_thread(function_to_execute)

    def register_products_trader(self, seller_id, product, quantity):
        with self._register_product:
            LOGGER.info(f"Inside register_products_trader()")
            warehouse_obj = self._network[self.num_peers-1]
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
