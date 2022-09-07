import pandas as pd
import numpy as np
from mellow_sdk.strategies import AbstractStrategy, UniV3Passive
from mellow_sdk.primitives import Pool, POOLS, MIN_TICK, MAX_TICK, Fee, Token
from collections import deque


class CustomStrategy(AbstractStrategy):
    """
    ``Custom Strategy`` is an active strategy with rebalances.
    """

    def __init__(self, name: str):
        super().__init__(name)
        self.window_size = 3000
        self.sliding_window = deque([], maxlen=self.window_size)
        self.std_dev = 0
        self.last_price_timestamp = 0
        self.strategy_risk_param = 4

    def push_to_sliding_window(self, price_row):
        self.sliding_window.append(price_row)
        return price_row

    def calculate_std_dev_from_sliding_window(self):
        self.std_dev = np.std(self.sliding_window)
        print("std_dev")
        print(self.std_dev)
        return

    def get_next_day_ranges(self, data):
        # iterate through pandas dataframe and push into sliding window all the prices
        swaps_data = data
        print("len(swaps_data['price'])")
        print(len(swaps_data["quotePrice"]))

        first_timestamp = swaps_data.head(1)["block_timestamp"]
        print("first_timestamp")
        print(first_timestamp)
        last_timestamp = swaps_data.tail(1)["block_timestamp"]
        print("last_timestamp")
        print(last_timestamp)

        # calculate the std dev from the sliding window
        swaps_data = swaps_data["quotePrice"].apply(self.push_to_sliding_window)
        self.calculate_std_dev_from_sliding_window()

        # get the last row price data
        last_price = swaps_data.tail(1)
        print("last_price")
        print(last_price)

        # based on the strategy parameters
        # add the std dev * strategy params onto the last row price data
        upper_price = last_price + (self.strategy_risk_param * self.std_dev)
        lower_price = last_price - (self.strategy_risk_param * self.std_dev)

        # output the upper and lower price ranges
        return upper_price, lower_price
        # use a function to convert the ranges into tick format(?)

    def rebalance(self, *args, **kwargs) -> str:
        return None
