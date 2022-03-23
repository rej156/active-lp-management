import os
from pathlib import Path
from decimal import Decimal
import subprocess
from datetime import datetime
import numpy as np
import polars as pl
import pandas as pd
from strategy.primitives import Pool
from strategy.utils import get_db_connector, ConfigParser
from binance import Client
import boto3
from strategy.utils import log


class PoolDataUniV3:
    """
    ``PoolDataUniV3`` contains data for backtesting.

    Attributes:
        pool: UniswapV3 ``Pool`` data
        mints: UniswapV3 mints data.
        burns: UniswapV3 burns data.
        swaps: UniswapV3 swaps data.
        swaps: UniswapV3 all events data.

    """
    def __init__(self,
                 pool: Pool,
                 mints: pl.DataFrame = None,
                 burns: pl.DataFrame = None,
                 swaps: pl.DataFrame = None,
                 full_df: pl.DataFrame = None,
                 ):

        self.pool = pool
        self.mints = mints
        self.burns = burns
        self.swaps = swaps
        self.full_df = full_df


class DownloadFromS3:
    def __init__(self, data_dir, bucket_name='mellow-public-data'):
        self.data_dir = data_dir
        self.bucket_name = bucket_name

    def check_dir(self):
        path_dir = Path(self.data_dir)
        if not path_dir.is_dir():
            log.info('Created directory', directory=self.data_dir)
            path_dir.mkdir(parents=True, exist_ok=True)

    def get_last_files(self):
        events = ['mint', 'burn', 'swap']
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        suffixes = []
        for file in bucket.objects.all():
            name = file.key
            if 'mint' in name or 'burn' in name or 'swap' in name:
                date = '-'.join(name.split('.')[0].split('/')[-1].split('-')[1:])
                suffixes.append(date)
        last_date = sorted(suffixes)[-1]

        files = []
        for event in events:
            file = last_date[:-3] + '/' + 'history-' + last_date + '.' + event + '.csv'
            files.append(file)
        return files

    def get_file_from_s3(self, file):
        file_name = '.'.join(file.split('.')[1:])
        s3client = boto3.client('s3')
        path = self.data_dir + '/' + file_name
        s3client.download_file(self.bucket_name, file, path)

    def download_files(self):
        self.check_dir()
        files = self.get_last_files()
        for file in files:
            log.info(f'Downloaded {file} from S3')
            self.get_file_from_s3(file)


class RawDataUniV3:
    """
        Load data from folder, preprocess and Return ``PoolDataUniV3`` instance.
    """
    def __init__(self, pool: Pool, data_dir):
        self.pool = pool
        self.data_dir = data_dir

    def check_files(self):
        path_mint = Path(f'{self.data_dir}/mint.csv')
        path_burn = Path(f'{self.data_dir}/burn.csv')
        path_swap = Path(f'{self.data_dir}/swap.csv')
        res = path_mint.is_file() and path_burn.is_file() and path_swap.is_file()
        return res

    def load_mints(self) -> pl.DataFrame:
        """
            Read mints from csv and preprocess

        Returns:
            mints df
        """
        mints_converters = {
            'pool': pl.Utf8,
            'block_hash': pl.Utf8,
            'tx_hash': pl.Utf8,
            'sender': pl.Utf8,
            'owner': pl.Utf8,
            "block_time": pl.Int64,
            "block_number": pl.Int64,
            'log_index': pl.Int64,
            "tick_lower": pl.Int64,
            "tick_upper": pl.Int64,
            "amount": pl.Float64,
            "amount0": pl.Float64,
            "amount1": pl.Float64,
        }
        file_name = f'{self.data_dir}/mint.csv'
        assert os.path.exists(file_name), f'File {file_name} does not exist.'

        df_mints_raw = pl.read_csv(file_name, dtypes=mints_converters)
        assert self.pool._address in df_mints_raw['pool'].unique(), f'Pool {self.pool._address} is not available yet.'
        df_mints = df_mints_raw.filter(pl.col('pool') == self.pool._address)

        df_prep = df_mints.select([
            pl.col('tx_hash'),
            pl.col('owner'),
            pl.col('block_number'),
            pl.col('log_index'),
            ((pl.col('block_time') * 1e3 + pl.col('log_index')) * 1e3).cast(pl.Datetime).alias('timestamp'),
            pl.col('tick_lower') + self.pool.tick_diff,
            pl.col('tick_upper') + self.pool.tick_diff,
            pl.col('amount0') / 10 ** self.pool.token0.decimals,
            pl.col('amount1') / 10 ** self.pool.token1.decimals,
            (pl.col('amount') / 10 ** self.pool.l_decimals_diff).alias('liquidity'),
        ]).with_column(
            pl.col('timestamp').dt.truncate("1d").alias('date')
        ).with_column(
            pl.Series(name='event', values=['mint'])
        ).sort(by=['block_number', 'log_index'])
        return df_prep

    def load_burns(self) -> pl.DataFrame:
        """
            Read burns from csv and preprocess

        Returns:
            burns df
        """
        burns_converters = {
            'pool': pl.Utf8,
            'block_hash': pl.Utf8,
            'tx_hash': pl.Utf8,
            'owner': pl.Utf8,
            "block_time": pl.Int64,
            "block_number": pl.Int64,
            'log_index': pl.Int64,
            "tick_lower": pl.Int64,
            "tick_upper": pl.Int64,
            "amount": pl.Float64,
            "amount0": pl.Float64,
            "amount1": pl.Float64,
        }
        file_name = f'{self.data_dir}/burn.csv'
        assert os.path.exists(file_name), f'File {file_name} does not exist.'
        df_burns_raw = pl.read_csv(file_name, dtypes=burns_converters)
        assert self.pool._address in df_burns_raw['pool'].unique(), f'Pool {self.pool._address} is not available yet.'
        df_burns = df_burns_raw.filter(pl.col('pool') == self.pool._address)

        df_prep = df_burns.select([
            pl.col('tx_hash'),
            pl.col('owner'),
            pl.col('block_number'),
            pl.col('log_index'),
            ((pl.col('block_time') * 1e3 + pl.col('log_index')) * 1e3).cast(pl.Datetime).alias('timestamp'),
            pl.col('tick_lower') + self.pool.tick_diff,
            pl.col('tick_upper') + self.pool.tick_diff,
            pl.col('amount0') / 10 ** self.pool.token0.decimals,
            pl.col('amount1') / 10 ** self.pool.token1.decimals,
            (pl.col('amount') / 10 ** self.pool.l_decimals_diff).alias('liquidity'),
        ]).with_column(
            pl.col('timestamp').dt.truncate("1d").alias('date')
        ).filter(
            (pl.col('amount0') + pl.col('amount1')) > 1e-6
        ).with_column(
            pl.Series(name='event', values=['burn'])
        ).sort(by=['block_number', 'log_index'])
        return df_prep

    def load_swaps(self) -> pl.DataFrame:
        """
            Read swaps from csv, preprocess, create sqrt_price_x96 column.

        Returns:
            swaps df
        """
        swaps_converters = {
            'pool': pl.Utf8,
            'block_hash': pl.Utf8,
            'tx_hash': pl.Utf8,
            'sender': pl.Utf8,
            'recipient': pl.Utf8,
            "block_time": pl.Int64,
            "block_number": pl.Int64,
            'log_index': pl.Int64,
            "tick": pl.Int64,
            "liquidity": pl.Float64,
            "amount0": pl.Float64,
            "amount1": pl.Float64,
            'sqrt_price_x96': pl.Float64,
        }
        file_name = f'{self.data_dir}/swap.csv'
        assert os.path.exists(file_name), f'File {file_name} does not exist.'

        df_swaps_raw = pl.read_csv(file_name, dtypes=swaps_converters)
        assert self.pool._address in df_swaps_raw['pool'].unique(), f'Pool {self.pool._address} is not available yet.'
        df_swaps = df_swaps_raw.filter(pl.col('pool') == self.pool._address)

        df_prep = df_swaps.select([
            pl.col('tx_hash'),
            pl.col('sender').alias('owner'),
            pl.col('block_number'),
            pl.col('log_index'),
            ((pl.col('block_time') * 1e3 + pl.col('log_index')) * 1e3).cast(pl.Datetime).alias('timestamp'),
            pl.col('amount0') / 10 ** self.pool.token0.decimals,
            pl.col('amount1') / 10 ** self.pool.token1.decimals,
            (pl.col('liquidity') / 10 ** self.pool.l_decimals_diff).alias('liquidity'),
            pl.col('tick') + self.pool.tick_diff,
            pl.col('sqrt_price_x96'),
        ]).sort(by=['block_number', 'log_index']).with_column(
            pl.col('timestamp').dt.truncate("1d").alias('date')
        ).with_column(
            pl.col("sqrt_price_x96").apply(
                lambda x: float((Decimal(x) * Decimal(x)) / (Decimal(2 ** 192) / Decimal(10 ** self.pool.decimals_diff)))).alias('price')
        ).with_columns([
            pl.col('price').shift_and_fill(1, pl.col('price').first()).alias('price_before'),
            pl.col('price').shift_and_fill(-1, pl.col('price').last()).alias('price_next')
        ]).with_column(
            pl.Series(name='event', values=['swap'])
        ).drop("sqrt_price_x96").sort(by=['block_number', 'log_index'])
        return df_prep

    def load_from_folder(self) -> PoolDataUniV3:
        """
            Load mints, burns, swaps from folder, preprocess and create ``PoolDataUniV3`` object and
            create all events df.

        Returns:
            `PoolDataUniV3`` object
        """
        if not self.check_files():
            downloader = DownloadFromS3(self.data_dir)
            downloader.download_files()

        mints = self.load_mints()
        burns = self.load_burns()
        swaps = self.load_swaps()

        full_df = (
                pl.concat([swaps, mints, burns], how='diagonal')
                .sort(by=['block_number', 'log_index'])
                .with_columns(
                [
                    pl.col('price').forward_fill().backward_fill(),
                    pl.col('tick').forward_fill().backward_fill()
                ]
            )
        )
        return PoolDataUniV3(self.pool, mints, burns, swaps, full_df)


class SyntheticData:
    """
    | ``SyntheticData`` generates UniswapV3 synthetic exchange data (swaps df).
    | Generates by sampling Geometric Brownian Motion.

    Attributes:
        pool:
            UniswapV3 ``Pool`` instance.
        start_date:
            Generating starting date. (example '1-1-2022')
        n_points:
            Amount samples to generate.
        init_price:
            Initial price.
        mu:
            Expectation of normal distribution.
        sigma:
            Variance of normal distribution.
        seed:
            Seed for random generator.
   """
    def __init__(
            self, pool, start_date: str = '1-1-2022', n_points: int = 365,
            init_price: float = 1, mu: float = 0, sigma: float = 0.1, seed=42):
        self.pool = pool
        self.start_date = start_date
        self.n_points = n_points

        self.init_price = init_price
        self.mu = mu
        self.sigma = sigma

        self.seed = seed

    def generate_data(self) -> PoolDataUniV3:
        """
        Generate synthetic UniswapV3 exchange data.

        Returns:
            ``PoolDataUniV3`` instance with synthetic swaps data, mint is None, burn is None.
        """
        timestamps = pd.date_range(start=self.start_date, periods=self.n_points, freq='D', normalize=True)
        # np.random.seed(self.seed)
        price_log_returns = np.random.normal(loc=self.mu, scale=self.sigma, size=self.n_points)
        price_returns = np.exp(price_log_returns)
        price_returns[0] = self.init_price

        prices = np.cumprod(price_returns)

        df = pd.DataFrame(zip(timestamps, prices), columns=['timestamp', 'price']).set_index('timestamp')

        df["price_before"] = df["price"].shift(1)
        df["price_before"] = df["price_before"].bfill()

        df["price_next"] = df["price"].shift(-1)
        df["price_next"] = df["price_next"].ffill()

        df = pl.from_pandas(df.reset_index())

        return PoolDataUniV3(self.pool, mints=None, burns=None, swaps=df, full_df=df)


class DownloaderBinanceData:
    """
        Download pair data from binance and write csv to /data folder.
    Args:
        pair_name: 'ethusdc' or other
        interval: Binance interval string, e.g.:
            '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w'
        start_str: string in format '%d-%M-%Y' (utc time format), (example '05-12-2018')
        end_str: string in format '%d-%M-%Y' (utc time format)
        config_path: path to yml config with config['binance']['api_key'], config['binance']['api_secret']
    Returns:
        pandas dataframe that was written to the /data/f'{pair_name}_{interval}_{start_str}_{end_str}.csv'
    """

    # Note
    # get_historical_klines
    # [
    #   [
    #     1499040000000,      // Open time - 0
    #     "0.01634790",       // Open
    #     "0.80000000",       // High
    #     "0.01575800",       // Low
    #     "0.01577100",       // Close - 4
    #     "148976.11427815",  // Volume
    #     1499644799999,      // Close time
    #     "2434.19055334",    // Quote asset volume
    #     308,                // Number of trades
    #     "1756.87402397",    // Taker buy base asset volume
    #     "28.46694368",      // Taker buy quote asset volume
    #     "17928899.62484339" // Ignore
    #   ]
    # ]
    def __init__(self, pair_name, interval, start_str, end_str, config_path, data_dir):
        self.pair_name = str.upper(pair_name)
        self.interval = interval
        self.start_str = start_str
        self.end_str = end_str
        self.config_path = config_path
        self.data_dir = data_dir

        subprocess.run(['mkdir', '-p', self.data_dir])

    def get(self) -> pd.DataFrame:
        # in - ms
        # in * 1000 - us
        # we need to get the num of sec in the interval
        map_dict = {'m': 1, 'h': 60, 'd': 24 * 60, 'w': 7 * 24 * 60}
        # num of nano sec [us]
        us_num = int(self.interval[0:-1]) * map_dict[self.interval[-1]] * 60 * 1000 * 1000

        # binance api client
        config = ConfigParser(config_path=self.config_path).config
        client = Client(
            config['binance']['api_key'],
            config['binance']['api_secret']
        )

        # download candles
        print('start:', datetime.now())
        try:
            klines = client.get_historical_klines(
                symbol=self.pair_name,
                interval=self.interval,
                start_str=self.start_str,
                end_str=self.end_str
            )
        except:
            assert False, 'using the api failed'
        print('finish:', datetime.now())
        print('rows downloaded:', len(klines))
        assert len(klines) > 1, '0 or 1 rows downloaded!'

        # preparing to timestamp to nano sec datetime[us]
        ts = [i[0] * 1000 for i in klines]
        index_col = list(range(ts[0], ts[-1] + us_num, us_num))

        # convert price to float
        price_col = [float(i[4]) for i in klines]

        # create dataframe
        df = pd.DataFrame({'price': [np.nan]}, index=index_col)
        df.index.name = 'timestamp'

        mismatch_rows = list(set(ts) - set(index_col))
        print('mismatch timestamp rows: ', len(mismatch_rows))

        data = np.array([ts, price_col])
        data = data[:, np.isin(data[0, :], mismatch_rows, invert=True)]

        df.loc[data[0, :], 'price'] = data[1, :]
        df.index = pd.to_datetime(df.index, unit='us')

        df['price'] = df['price'].shift(1)
        df = df.iloc[1:]
        df = df.reset_index()

        file_name = f'{self.pair_name}_{self.interval}_{self.start_str}_{self.end_str}.csv'
        file_path = os.path.join(self.data_dir, file_name)

        df.to_csv(file_path, index=False, date_format='%Y-%m-%d %H:%M:%S:%f')
        print(f'df shape {df.shape} saved to {file_path}')
        return df
