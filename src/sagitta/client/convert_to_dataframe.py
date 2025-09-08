"""
" Functions to process raw market information into
" pandas dataframes
"
" @author: Michael Kane
" @date:   07/09/2025
"""
import pandas as pd


def processKlines(
        klines
        ):
    """
    Process kline data into pandas dataframe

    Args:
        klines:
            Kline data retrieved from Binance
    Returns:
        kline_df:
            Kline data in pandas dataframe
    """

    # Convert kline data into a pandas dataframe with these labels
    klines_df = pd.DataFrame(
        klines,
        columns=[ 
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume", 
            "close_time",
            "quote_asset_volume",
            "trades", 
            "taker_buy_base",
            "taker_buy_quote",
            "ignore" ]
            )
    
    # Convert select columns to float type
    for col in ["open", "high", "low", "close", "volume", "close"]:
        klines_df[col] = klines_df[col].astype( float )
    
    return klines_df