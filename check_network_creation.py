from enums.item_type import Item
from enums.peer_type import PeerType
from peer.network import NetworkCreator
from peer.peer_generator import PeerGenerator






if __name__ == "__main__":
    item_quantities_map = {
        Item.FISH: 10,
        Item.SALT: 20,
        Item.BOAR: 15,
    }

    peer_generator = PeerGenerator(num_peers=10,
                                   peer_types=PeerType.get_values(),
                                   item_quantities_map=item_quantities_map,
                                   max_hop_count=10
                                   )
    peer_generator = NetworkCreator(peer_generator=peer_generator)
    print(peer_generator.generate_network())
