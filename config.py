from enums.item_type import Item

item_quantities_map = {
    Item.FISH: (10,10),
    Item.SALT: (20,15),
    Item.BOAR: (15,30),
}

thread_pool_size = 20
buyer_timeout_s = 30
buyer_pool_interval_s = 0.5
buyer_max_loops = int(buyer_timeout_s / buyer_pool_interval_s)
buyer_item_switch_delay_s = 3
params_pickle_file_path = "/tmp/params.pkl"