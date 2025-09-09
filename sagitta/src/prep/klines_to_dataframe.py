"""
" Functions to process raw market information into
" pandas dataframes
"
" @author: Michael Kane
" @date:   07/09/2025
"""
import pandas as pd


def convertKlinesToDataframe(
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
    df = pd.DataFrame(
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
    
    # Convert relevant columns from strings to float data
    for col in ["open", "high", "low", "close", "volume", "taker_buy_base", "taker_buy_quote"]:
        df[ col ] = df[ col ].astype( float )

    # Convert relevant columns from strings to int
    df[ "trades" ] = df[ "trades" ].astype( int )

    df[ "timestamp" ]  = pd.to_datetime( df[ "timestamp" ],  unit="ms", utc=True )
    df[ "close_time" ] = pd.to_datetime( df[ "close_time" ], unit="ms", utc=True )
    df.drop( columns=[ "ignore" ], inplace=True )
    
    return df