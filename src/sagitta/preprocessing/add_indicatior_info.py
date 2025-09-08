"""
" Functions to create indicator data from raw 
" market dataframe
"
" @author: Michael Kane
" @date:   08/09/2025
"""
import pandas as pd
import numpy as np


def addFuturePriceColumn(
        df,
        steps
        ):
    """
    Add column to kline dataframe for the close price "steps" amount
    in the future. Caculate percent change over a that period.
     ____________________
    |__E.g:_for_steps=1__|
    |__£Close___£Future__|
    |    0    |    1     |
    |    1    |    2     |
    |____2____|___nan____|

    Args:
        df:
            Kline data held in a pandas dataframe
        steps:
            Number of steps in the future for the future price
    Returns:
        df:
            Edited kline data with 'future_price' amd 'pct_change'
            column
    """

    # Build future price column
    df["future_price"] = df["close"].shift( -steps )

    # Calculare percentage change
    df["pct_change"] = (df["future_price"] - df["close"]) / df["close"]

    return df


def addBinaryLabel(
        df,
        threshold,
        ):
    """
    Gives pct_change as 0 (false) if below threshold and 1 (true) if
    above threshold. Important for binary prediction.

    Args:
        df:
            A pandas dataframe containing market information
        threshold:
            Define percentage required for label to be 1 or 0
    Returns:
        df:
            Returns the same dataframe with the additional
            pct_change
    """

    # Check column to see if pct_change is there
    if "future_price" not in df.columns:
        raise KeyError("DataFrame is missing required column: 'future_price'")
    if "pct_change" not in df.columns:
        raise KeyError("DataFrame is missing required column: 'pct_change'")
    
    # Label the binary label
    df["binary_label"] = (df["pct_change"] >= threshold).astype(int)

    return df


def addEMA(
        df,
        lower_period,
        upper_period
        ):
    """
    Add Exponential Moving Average (EMA) information and typicall is indicative
    of trend. At time t, feature column will be calculated including the price
    at time t, .shift(1) moves the feature 'back one' to align.

    Args:
        df:
            Raw market data dataframe
        lower_period:
            Period to calculate shorter EMA over
        upper_period:
            Period to calculate longer EMA over
    Returns:
        df:
            Adjusted market dataframe
    """

     # Calculate EMA's
    df[ f"ema_{lower_period}" ] = df[ "close" ].ewm( span=lower_period, adjust=False ).mean().shift( 1 )
    df[ f"ema_{upper_period}" ] = df[ "close" ].ewm( span=upper_period, adjust=False ).mean().shift( 1 )

    return df


def addMomIndicator(
        df,
        lower_period,
        upper_period,
        ):
    """
    Add momentum indicator information based on close prices.

    Args:
        df:
            Raw market data dataframe
        lower_period:
            Period to calculate shorter momentum difference over
        upper_period:
            Period to calculate longer momentum difference over
    Returns:
        df:
            Adjusted market dataframe
    """
    # Calculate momentum differences for defined period
    df[ f"momentum_{lower_period}" ] = df[ "close" ].diff( periods=lower_period ).shift(1)
    df[ f"momentum_{upper_period}" ] = df[ "close" ].diff( periods=upper_period ).shift(1)

    return df


def addMACD(
        df,
        lower_period,
        upper_period,
        signal_period
        ):
    """
    Add MACD (Moving Average Convergence Divergence) information.
    MACD is a momentum indicator.

    Args:
        df:
            Raw market data dataframe
        lower_period:
            Period to calculate shorter EMA over
        upper_period:
            Period to calculate longer EMA over
        signal_period:
            Window over which to calculate MACD signal
    Return:
        df:
            Adjusted market dataframe
    """
    
    # Exponential moving averages (EMA) over a 12 and 26 period span
    lower_ema = df[ "close" ].ewm( span=lower_period, adjust=False ).mean()
    upper_ema = df[ "close" ].ewm( span=upper_period, adjust=False ).mean()

    # Subtract slow from fast EMA -> difference is the MACD line
    df[ "macd" ] = ( lower_ema - upper_ema ).shift( 1 )

    # Takes MACD line and compute 9-period EMA, producing the signal line
    df[ "macd_signal" ] = df[ "macd" ].ewm( span=signal_period, adjust=False ).mean().shift( 1 )

    return df


def addBB(
        df
        ):
    """
    Add Bollinger Band indicator information - a type of volatility indicator.

    Args:
        df:
            Raw market data dataframe
    Return:
        df:
            Adjusted market dataframe
    """

    # Create a 20-period rolling window on closing prices
    rolling20 = df[ "close" ].rolling( window=20 )

    # Get mean and standard deviation of the rolling window
    df[ "bb_mid" ] = rolling20.mean().shift( 1 )
    df[ "bb_std" ] = rolling20.std().shift( 1 )

    # 2 sigma deviation from mean = upper and lower bands, narrow bands ~ low volatility
    df[ "bb_upper" ] = df[ "bb_mid" ] + 2 * df[ "bb_std" ] # Possibly oversold
    df[ "bb_lower" ] = df[ "bb_mid" ] - 2 * df[ "bb_std" ] # Possibly undersold

    return df


def addRSI(
        df,
        RSI_period
        ):
    """
    Add RSI (Relative Strength Index) information - a momentum indicator.

    Args:
        df:
            Raw market data dataframe
        RSI_period:
            Window RSI is calculated over
    Return:
        df:
            Adjusted market dataframe
    """

    # Calculates candle-to-candle change in closing prices
    delta = df[ "close" ].diff()

    # Keeps posititves, anything below 0 set to 0
    gain = delta.clip( lower=0 )
    # Keeps negatives, anything above 0 set to 0
    loss = -delta.clip( upper=0 )

    # Calculare rolling mean over window period for both
    avg_gain = gain.rolling( window=RSI_period ).mean()
    avg_loss = loss.rolling( window=RSI_period ).mean()

    # Their ratio gives their relative strength
    rs = avg_gain / avg_loss
    # RSI formula, values range for 0->100, underbought->overbought
    df[ "rsi" ] = ( 100 - (100 / (1+rs)) ).shift( 1 )

    return df


def addATR(
        df,
        ATR_period
        ):
    """
    Add ATR (Average True Range) information - a volatility indicator.

    Args:
        df:
            Raw market data dataframe
        ATR_period:
            Window ATR is calculated over
    Return:
        df:
            Adjusted market dataframe
    """

    # Calculate |high-low| price
    high_low = df[ "high" ] - df[ "low" ]
    # Caclulate |high-close| price
    high_close = ( df["high"] - df[ "close" ].shift() ).abs()
    # Calculate |low-close| price
    low_close = ( df["low"] - df[ "close" ].shift() ).abs()

    # True Range = max of the three
    true_range = pd.concat( [ high_low, high_close, low_close ], axis=1 ).max( axis=1 )
    # ATR = exponential moving average of 'true range'
    df["atr"] = true_range.ewm(span=ATR_period, adjust=False).mean().shift(1)

    return df


def addOBV(
        df
        ):
    """
    Add OBV (On Balance Volume) information - a volume-based indicator
    often used to confirm momentum. Becomes a running total of 'signed volume'.
    Rising suggests buying pressure (vol mostly on up days).
    Falling suggests selling pressure (vol mostly on down days).

    Args:
        df:
            Raw market data dataframe
    Return:
        df:
            Adjusted market dataframe
    """

    # Calculate diff in candle-to-candle close and convert direction to sign [up=+&down=-]
    direction = np.sign( df[ "close" ].diff() )
    # Direction * volume = sign based volume measure, fill NaNs with 0 and take cumulative sum
    df["obv"] = ( direction * df[ "volume" ] ).fillna( 0 ).cumsum()

    return df


def addStochasticOsc(
        df,
        window
        ):
    """
    Adds Stochastic Oscillator information, a momentum indicator.

    Args:
        df:
            Raw market data dataframe
        window:
            Period to calculate rolling min and max's over
    Return:
        df:
            Adjusted market dataframe
    """

    # The minimum low over the past window bars.
    lowest_low = df[ "low" ].rolling( window=window ).min()
    # The largest high over the past window bars.
    highest_high = df[ "high" ].rolling( window=window ).max()

    # Formula for %K. E.g: >80% suggests overshold and <20% is undersold.
    df[ "stochastic_k" ] = 100 * ( (df[ "close" ] - lowest_low) / (highest_high - lowest_low) ).shift(1)

    return df


def addCCI(
        df,
        window
        ):
    """
    Add Commodity Channel Index (CCI) information, a momentum based indicator.
    Larger values (+100) indicates overbought and lower (-100) values indicate
    underbought.

    Args:
        df:
            Raw market data dataframe
    Return:
        df:
            Adjusted market dataframe
    """

    # Calculate average of high, low and close
    typical_price = ( df[ "high" ] + df[ "low" ] + df[ "close" ] ) / 3
    # Calculare rolling average over 'window' for typical price (TP)
    mean_tp = typical_price.rolling( window=window ).mean()
    # Calculate Mean Absolute Deviation - how far on average each TP is from the mean of the window
    mad_tp = typical_price.rolling( window=window ).apply( lambda x: np.mean( np.abs( x-np.mean(x) ) ) )
    # Formula for CCI, 0.015 is a typical scale factor chosen to limit values -100<val<100
    df[ "cci" ] = ( (typical_price-mean_tp) / (0.015*mad_tp) ).shift( 1 )

    return df