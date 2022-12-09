import enum


class Item(str,enum.Enum):
    FISH = "fish"
    SALT = "salt"
    BOAR = "boar"


available_items = [ele for ele in Item]
