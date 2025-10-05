"""
" Functionality to clean raw market dataframe
"
" @author: Michael Kane
" @date:   14/09/2025
"""
import pandas as pd
import numpy as np


def normalizeDtypes(
        df
        ):
    """
    Normalize Binance kline dataframe dtypes:
    - Convert OHLCV and related fields to numeric
    - Coerce bad values to NaN
    - Ensure trades are nullable Int64
    """

    # Ensure numeric dtypes; Binance often returns strings
    num_cols = [ "open", "high", "low", "close", "volume", "quote_asset_volume", "taker_buy_base", "taker_buy_quote" ]

    # Loop through columns
    for c in num_cols:
        # Overwrite original column with best numeric data type, set to NaN if datatype is not known
        df[c] = pd.to_numeric( df[c], errors="coerce" )

    # Number of trades stays as integer
    df[ "number_of_trades" ] = pd.to_numeric( df["number_of_trades"], errors="coerce" ).astype("Int64")


    return df


def makeTimeIndex(
        df
        ):
    """
    Set correct datatype for time
    - Convert ms timestamps to UTC datetime
    """

    # Set open time
    df["open_time"] = pd.to_datetime( df["open_time"], unit="ms", utc=True )

    # Set close time 
    df["close_time"] = pd.to_datetime( df["close_time"], unit="ms", utc=True )

    # Set the open time as the df index
    df.index = df["open_time"]

    # Sort by open time
    df = df.sort_index()
    
    return df


def dropDupes(
        df
        ):
    """
    Drop rows which contain duplicates of the time column
    """

    # Length before
    before = len(df)

    # For any duplicated rows, remove all but last
    deduped = df[~df.index.duplicated(keep="last")]

    # Calculate before and after
    after = len(deduped) 
    dropped = before - after

    print( f" [CLEAN] Dropped {dropped} duplicate rows based on time index." )

    return deduped


def checkOHLC(
        df
        ):
    """
    Check the contents of the OHLC columns for anything inconsistent
    """

    # Grab OHLC columns
    o = df[ "open" ]; h = df[ "high" ]; l = df[ "low" ]; c = df[ "close" ]

    # Get mask for where h<l
    swapped = h < l
    n_swap = int( swapped.sum() )

    # If any h<l, swap the entries in that column for the rows
    if swapped.any():
        # Swap where h<l
        df.loc[ swapped, ["high", "low"] ] = df.loc[ swapped, ["low", "high"] ].values

    # Upper clamp of the max bounds per candle
    max_oc = np.maximum( df["open"], df["close"] )
    min_oc = np.minimum( df["open"], df["close"] )

    # For logging
    n_high_fix = int( (df["high"] < max_oc).sum() )
    n_low_fix  = int( (df["low"]  > min_oc).sum() )

    # Lower clamp
    df[ "high" ] = np.where( df["high"] < max_oc, max_oc, df["high"] )
    df[ "low" ]  = np.where( df["low"]  > min_oc, min_oc, df["low"] )

    print( f' [CLEAN] OHLC check - HL swapped: {n_swap}, Clamped highs: {n_high_fix}, Clamped lows: {n_low_fix}' )

    return df


def removeNegsAndNaNs(
        df,
        columns = [ "open", "high", "low", "close", "volume", "quote_asset_volume", "taker_buy_base", "taker_buy_quote" ]
        ):
    """
    Remove negatives and NaNs from select columns
    Adds logging for number of rows dropped at each stage.
    """

    # Get length of fd
    before = len(df)

    # Remove rows with NaNs
    df_nan = df.dropna( subset=[c for c in columns if c in df.columns] )

    # Check length after
    after_nan = len(df_nan)
    dropped_nan = before - after_nan

    print(f" [CLEAN] Dropped {dropped_nan} rows with NaNs.")

    # For logging
    before_ohlc = after_nan
    df_ohlc = df_nan

    # Loop through OHLC columns
    for p in [ "open", "high", "low", "close" ]:

        # Remove negatives
        df_ohlc = df_ohlc[ df_ohlc[p] > 0 ]

    # Length afterwards
    after_ohlc = len(df_ohlc)
    dropped_ohlc = before_ohlc - after_ohlc

    print(f" [CLEAN] Dropped {dropped_ohlc} rows with negative OHLC values.")

    # Remove negative volumes
    before_vol = after_ohlc
    df_vol = df_ohlc

    # Remove negative volumes
    df_vol = df_vol[ df_vol["volume"] >= 0 ]

    # Get length after
    after_vol = len(df_vol)
    dropped_vol = before_vol - after_vol

    print(f" [CLEAN] Removing negatives and NaNs - NaN dropped: {dropped_nan}, Negative OHLC dropped: {dropped_ohlc}, Negative volume dropped: {dropped_vol}, Total remaining: {after_vol}" )

    return df_vol