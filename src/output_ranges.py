from mellow_sdk.data import RawDataUniV3
from custom_strategy import CustomStrategy
import pandas as pd


def get_pool_data():
    data = pd.read_csv("data/swaps.csv")
    data = data.sort_values(by="block_timestamp")
    return data


def main():
    data = get_pool_data()
    strat = CustomStrategy(name="custom")
    upper_price, lower_price = strat.get_next_day_ranges(data)

    print("upper_price")
    print(upper_price.to_string())
    print("lower_price")
    print(lower_price.to_string())
    print("1/upper_price")
    print(1 / upper_price)
    print("1/lower_price")
    print(1 / lower_price)


if __name__ == "__main__":
    main()
