from typing import List, Tuple
from datetime import datetime
import numpy as np

from strategy.positions import BiCurrencyPosition, AbstractPosition, UniV3Position
from strategy.uniswap_utils import UniswapLiquidityAligner


class Portfolio(AbstractPosition):
    """
    ``Portfolio`` is a container for several open positions.

    Attributes:
        name: Unique name for the position.
        positions: List of initial positions.
    """

    def __init__(
            self, name: str, rebalance_cost, swap_fee, fee_percent, x_interest=None,
            y_interest=None,positions: List[AbstractPosition] = None
    ):
        super().__init__(name)

        if positions is None:
            positions = []
        self.positions = {pos.name: pos for pos in positions}

        self.swap_fee = swap_fee
        self.fee_percent = fee_percent
        self.x_interest = x_interest
        self.y_interest = y_interest
        self.rebalance_cost = rebalance_cost

        self.positions['main_vault'] = BiCurrencyPosition(
            name='main_vault',
            swap_fee=self.swap_fee,
            rebalance_cost=self.rebalance_cost,
            x=0,
            y=0,
            x_interest=self.x_interest,
            y_interest=self.y_interest
        )

        self.cf_in_x = 0
        self.cf_in_y = 0
        self.cf_out_x = 0
        self.cf_out_y = 0

    def deposit(self, x, y):
        self.positions['main_vault'].deposit(x, y)
        self.cf_in_x += x
        self.cf_in_y += y

    def withdraw(self, x, y):
        self.positions['main_vault'].withdraw(x, y)
        self.cf_out_x += x
        self.cf_out_y += y

    def rename_position(self, current_name: str, new_name: str) -> None:
        """
        Rename position in portfolio by its name.

        Args:
            current_name: Current name of position.
            new_name: New name for position.
        """
        self.positions[current_name].rename(new_name)
        self.positions[new_name] = self.positions.pop(current_name)

    def append(self, position: AbstractPosition) -> None:
        """
        Add position to portfolio.

        Args:
            position: Any ``AbstractPosition`` instance.
        """
        self.positions[position.name] = position

    def remove(self, name: str) -> None:
        """
        Remove position from portfolio by its name.

        Args:
            name: Position name.
        """
        if name not in self.positions:
            raise Exception(f'Invalid name = {name}')
        del self.positions[name]

    def get_position(self, name: str) -> AbstractPosition:
        """
        Get position from portfolio by name.

        Args:
            name: Position name.

        Returns:
            ``AbstractPosition`` instance.
        """
        return self.positions.get(name, None)

    def get_last_position(self) -> AbstractPosition:
        """
        Get last position from portfolio.

        Returns:
             Last position in portfolio.
        """
        if self.positions:
            last_key = list(self.positions.keys())[-1]
            pos = self.get_position(last_key)
            return pos
        else:
            raise Exception('Position not found')

    def positions_list(self) -> List[AbstractPosition]:
        """
        Get list of all positions in portfolio.

        Returns:
            List of all positions in portfolio.
        """
        return list(self.positions.values())

    def position_names(self) -> List[str]:
        """
        Get list of position names in portfolio.

        Returns:
            List of all position names in portfolio.
        """
        return list(self.positions.keys())

    def to_x(self, price: float) -> float:
        """
        Get total value of portfolio denominated to X.

        Args:
            price: Current price of X in Y currency

        Returns:
            Total value of portfolio denominated in X
        """
        total_x = 0
        for _, pos in self.positions.items():
            total_x += pos.to_x(price)
        return total_x

    def to_y(self, price: float) -> float:
        """
        Get total value of portfolio expressed in Y

        Args:
            price: Current price of X in Y currency

        Returns:
            Total value of portfolio denominated in Y
        """
        total_y = 0
        for _, pos in self.positions.items():
            total_y += pos.to_y(price)
        return total_y

    def to_xy(self, price: float) -> Tuple[float, float]:
        """
        Get amount of X and amount of Y in portfolio

        Args:
            price: Current price of X in Y currency.

        Returns:
            (amount of X, amount of Y)
        """
        total_x = 0
        total_y = 0
        for _, pos in self.positions.items():
            x, y = pos.to_xy(price)
            total_x += x
            total_y += y
        return total_x, total_y

    def snapshot(self, timestamp: datetime, price: float) -> dict:

        """
        | Get portfolio snapshot.
        | Used in PortfolioHistory.add_snapshot() to collect backtest data.

        Args:
            timestamp: Timestamp of snapshot
            price: Current price of X in Y currency

        Returns: Position snapshot
        """
        snapshot = {'timestamp': timestamp, 'price': price}
        for _, pos in self.positions.items():
            snapshot.update(pos.snapshot(timestamp, price))
        return snapshot
