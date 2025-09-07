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


def addFuturePriceColumn(
        klines_df,
        steps
        ):
    """
    Add column to kline dataframe for the close
    price "steps" amount in the future

    E.g: for steps=1
    Close future_price
      0         1
      1         2
      2        nan

    Args:
        klines_df:
            Kline data held in a pandas dataframe
        steps:
            Number of steps in the future for the future price
    Returns:
        klines_df:
            Edited kline data with "future_price" column
    """

    # Build future price column
    klines_df["future_price"] = klines_df["close"].shift( -steps )

    return klines_df