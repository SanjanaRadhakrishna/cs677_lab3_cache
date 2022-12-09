import pickle
import random
from socket import socket

from enums.item_type import Item, available_items


def get_new_item(current_item: Item):
    current_item = current_item
    count = len(available_items)

    for i in range(count):
        selected_item = random.choice(available_items)
        if current_item != selected_item:
            return selected_item

    return current_item


def get_free_port():
    with socket() as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def pickle_load_from_file(file_path: str):
    with open(file_path, "rb") as f:
        return pickle.load(f)


def pickle_to_file(file_path: str, data):
    with open(file_path, "wb") as f:
        pickle.dump(data, f)
