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
    Process raw kline data into pandas dataframe

    Args:
        klines:
            Kline data retrieved from Binance
    Returns:
        kline_df:
            Kline data in pandas dataframe
    """

    print( f' (PREP) Converting raw binance data to pandas df... ', end="" )

    # Convert kline data into a pandas dataframe with these labels
    df = pd.DataFrame(
        klines,
        columns=[ 
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume", 
            "close_time",
            "quote_asset_volume",
            "number_of_trades", 
            "taker_buy_base",
            "taker_buy_quote",
            "ignore" ]
            )
    
    # Drop the ignore column
    df.drop( columns=[ "ignore" ], inplace=True )
    
    return df