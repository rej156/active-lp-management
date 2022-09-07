from mellow_sdk.primitives import Pool, POOLS, MIN_TICK, MAX_TICK, Fee, Token
from mellow_sdk.data import RawDataUniV3
from custom_strategy import CustomStrategy


def get_pool_data(pool):
    data = RawDataUniV3(
        pool=pool, data_dir="data", reload_data=False
    ).load_from_folder()
    return data


def get_pool():
    return Pool(tokenA=Token.USDC, tokenB=Token.WETH, fee=Fee.MIDDLE)


def main():
    pool = get_pool()
    data = get_pool_data(pool)
    strat = CustomStrategy(name="custom")
    upper_price, lower_price = strat.get_next_day_ranges(data)

    print("upper_price")
    print(upper_price)
    print("lower_price")
    print(lower_price)


if __name__ == "__main__":
    main()
