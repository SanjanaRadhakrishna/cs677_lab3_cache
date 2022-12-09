from typing import List, Dict

from enums.item_type import Item
from enums.peer_type import PeerType
from peer.model import Peer
from utils import get_free_port


class NetworkCreator:
    def __init__(self, nodes: List[Peer]):
        self._nodes: List[Peer] = nodes

    """
        {
            "p1": <Peer>,
            "p2": <Peer>
            ....
        }
    """

    def generate_network(self) -> Dict[str, Peer]:
        network_dict = {}
        num_nodes = len(self._nodes)

        #   for i, node in enumerate(self._nodes):
        # next_neighbour_index = (i + 1) % num_nodes
        # last_neighbour_index = (i - 1) % num_nodes

        # next_neighbour = self._nodes[next_neighbour_index]
        # prev_neighbour = self._nodes[last_neighbour_index]

        # # Form connections for all neighbors
        # node.add_neighbour(next_neighbour.id)
        # node.add_neighbour(prev_neighbour.id)

        # for j in range(num_nodes):
        #     if (j!=i):
        #         neighb_id = self._nodes[i]
        #         node.add_neighbour(neighb_id)

        network_dict = {0: Peer(id=0,
                                host="localhost",
                                neighbours=[1, 2, 3, 4, 5],
                                peer_type=PeerType.SELLER,
                                item=Item.SALT,
                                available_item_quantity=3,
                                amt_earned=0,
                                amt_spent=0,
                                commission=0,
                                port=get_free_port(),
                                trader_list = []


                                ),

                        1: Peer(id=1,
                                host="localhost",
                                neighbours=[0, 2, 3, 4, 5],
                                peer_type=PeerType.BUYER,
                                item=Item.SALT,
                                available_item_quantity=0,
                                amt_earned=0,
                                amt_spent=0,
                                commission=0,
                                port=get_free_port(),
                                trader_list = []

                                ),
                        2: Peer(id=2,
                                host="localhost",
                                neighbours=[0, 1, 3, 4, 5],
                                peer_type=PeerType.SELLER,
                                item=Item.BOAR,
                                available_item_quantity=3,
                                amt_earned=0,
                                amt_spent=0,
                                commission=0,
                                port=get_free_port(),
                                trader_list = []

                                ),
                        3: Peer(id=3,
                                host="localhost",
                                neighbours=[0, 1, 2, 4, 5],
                                peer_type=PeerType.BUYER,
                                item=Item.BOAR,
                                available_item_quantity=0,
                                amt_earned=0,
                                amt_spent=0,
                                commission=0,
                                port=get_free_port(),
                                trader_list = []
                                ),
                        4: Peer(id=4,
                                host="localhost",
                                neighbours=[0, 1, 2, 3, 5],
                                peer_type=PeerType.BUYER,
                                item=Item.BOAR,
                                available_item_quantity=0,
                                amt_earned=0,
                                amt_spent=0,
                                commission=0,
                                port=get_free_port(),
                                trader_list = []

                                ),
                        5: Peer(id=5,
                                host="localhost",
                                neighbours=[0, 1, 2, 3, 4],
                                peer_type=PeerType.BUYER,
                                item=Item.SALT,
                                available_item_quantity=0,
                                amt_earned=0,
                                amt_spent=0,
                                commission=0,
                                port=get_free_port(),
                                trader_list = []

                                ),
                        6: Peer(id=6,
                                host="localhost",
                                neighbours=[0, 1, 2, 3, 4],
                                peer_type=PeerType.WAREHOUSE,
                                item=None,
                                available_item_quantity=0,
                                amt_earned=0,
                                amt_spent=0,
                                commission=0,
                                port=get_free_port(),
                                trader_list = []

                                )
                        }

        return network_dict

    def print(self, network):
        for _, peer in network.items():
            peer.print()
