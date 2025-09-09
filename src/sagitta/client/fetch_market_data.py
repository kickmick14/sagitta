"""
" Fetch k-line (candlestick) data for a given crypto pair
"
" @author: Michael Kane
" @date:   07/09/2025
"""
import pandas as pd


def fetchKlineData(
        client, 
        pair,   
        period,
        start
        ):
    """
    Get kline data from binance client

    Args:
        client:
            Binance client
        pair:
            Pair to be traded
        period:
            Timeperiod of each kline
        start:
            How far back to take data from
    Returns:
        klines:
            Unprocessed kline information from client
    """

    # Retrieve klines from binnace client
    klines = client.get_historical_klines( pair, period, start )

    return klines