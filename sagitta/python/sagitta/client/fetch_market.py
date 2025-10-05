"""
" Fetch k-line (candlestick) data for a given crypto pair
"
" @author: Michael Kane
" @date:   07/09/2025
"""
from datetime import date

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

    print( f" [CLIENT] Fetching market k-lines for... {pair} {period} on {date.today()} from {start} ago" )

    # Retrieve klines from binnace client
    klines = client.get_historical_klines( pair, period, start )

    print( f" [CLIENT] Received k-lines" )

    return klines