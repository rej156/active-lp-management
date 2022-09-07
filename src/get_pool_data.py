import pandas as pd
from datetime import date, datetime, timedelta
import requests
import os

import config

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.GOOGLE_SERVICE_AUTH_JSON


def download_bigquery_swap_data(
    contract_address,
    date_begin,
    date_end,
    decimals_0,
    decimals_1,
    network="ethereum",
    block_start=0,
):
    """
    Internal function to query Google Bigquery for the swap history of a Uniswap v3 pool between two dates starting from a particular block from Ethereum Mainnet.
    Use GetPoolData.get_pool_data_bigquery which preprocesses the data in order to conduct simualtions with the Active Strategy Framework.
    """

    from google.cloud import bigquery

    client = bigquery.Client()

    query = f"""
            SELECT *
            FROM blockchain-etl.{network}_uniswap.UniswapV3Pool_event_Swap
            where contract_address = lower('{contract_address}') and
              block_timestamp >= '{date_begin}' and block_timestamp <= '{date_end}' and block_number >= {block_start}
            """
    query_job = client.query(query)  # Make an API request.
    resulting_data = query_job.to_dataframe(create_bqstorage_client=False)
    DECIMAL_ADJ = 10 ** (decimals_1 - decimals_0)
    resulting_data["sqrtPriceX96_float"] = resulting_data["sqrtPriceX96"].astype(float)
    resulting_data["quotePrice"] = (
        ((resulting_data["sqrtPriceX96_float"] / 2**96) ** 2) / DECIMAL_ADJ
    ).astype(float)
    resulting_data["block_date"] = pd.to_datetime(resulting_data["block_timestamp"])
    return resulting_data


# EXAMPLE USAGE
eth_usdc_pool_address = str("0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8").lower()
price_data_begin = str("2021-05-04")
price_data_end = str("2022-09-08")
decimals_0 = 6
decimals_1 = 18

df = download_bigquery_swap_data(
    eth_usdc_pool_address, price_data_begin, price_data_end, decimals_0, decimals_1
)
df.to_csv("data/swaps.csv")
print(df.tail())
