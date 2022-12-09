import enum


class PeerType(str,enum.Enum):
    BUYER = "BUYER"
    SELLER = "SELLER"
    TRADER = "TRADER"
    WAREHOUSE = "WAREHOUSE"


peer_types = [ele for ele in PeerType]
