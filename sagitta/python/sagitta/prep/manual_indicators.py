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
    Add Exponential Moving Average (EMA) information and typically is indicative
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
    Returns:
        df:
            Adjusted market dataframe
    """
    
    # Exponential moving averages (EMA) over a 12 and 26 period span
    lower_ema = df[ "close" ].ewm( span=lower_period, adjust=False ).mean()
    upper_ema = df[ "close" ].ewm( span=upper_period, adjust=False ).mean()

    # Subtract slow from fast EMA -> difference is the MACD line
    macd = lower_ema - upper_ema

    # Takes MACD line and compute 9-period EMA, producing the signal line
    signal = macd.ewm( span=signal_period, adjust=False ).mean()

    # Calculate and then shift
    df["macd"] = macd.shift( 1 )
    df["macd_signal"] = signal.shift( 1 )

    return df


def addBB(
        df,
        period
        ):
    """
    Add Bollinger Band indicator information - a type of volatility indicator.

    Args:
        df:
            Raw market data dataframe
        period:
            Period to calculate bollinger bands over
    Returns:
        df:
            Adjusted market dataframe
    """

    # Create a rolling window on closing prices
    rolling = df[ "close" ].rolling( window=period )

    # Get mean and standard deviation of the rolling window
    df[ f"bb_{period}_mean" ] = rolling.mean().shift( 1 )
    df[ f"bb_{period}_std" ] = rolling.std().shift( 1 )

    # 2 sigma deviation from mean = upper and lower bands, narrow bands ~ low volatility
    df[ f"bb_{period}_upper" ] = df[ f"bb_{period}_mid" ] + 2 * df[ f"bb_{period}_std" ] # Possibly overbought
    df[ f"bb_{period}_lower" ] = df[ f"bb_{period}_mid" ] - 2 * df[ f"bb_{period}_std" ] # Possibly oversold

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
    Returns:
        df:
            Adjusted market dataframe
    """

    # Calculates candle-to-candle change in closing prices
    delta = df[ "close" ].diff()

    # Keeps positives (negatives), anything below (above) 0 set to 0
    gain = delta.clip( lower=0 )
    loss = -delta.clip( upper=0 )

    # Calculate rolling mean over window period for both
    avg_gain = gain.rolling( window=RSI_period ).mean()
    avg_loss = loss.rolling( window=RSI_period ).mean()

    # Their ratio gives their relative strength
    rs = avg_gain / avg_loss
    # RSI formula, values range for 0->100, oversold->overbought
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
    Returns:
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
    df["atr"] = true_range.ewm(span=ATR_period, adjust=False).mean().shift( 1 )

    return df


def addOBV(
        df
        ):
    """
    Add OBV (On Balance Volume) information - a volume-based indicator
    often used to confirm momentum. Becomes a running total of 'signed
    volume'. Rising suggests buying pressure (vol mostly on up days).
    Falling suggests selling pressure (vol mostly on down days).

    Args:
        df:
            Raw market data dataframe
    Returns:
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
        period,
        smooth_period
        ):
    """
    Adds Stochastic Oscillator information (%K and %D), a momentum indicator.

    Args:
        df:
            Raw market data dataframe
        period:
            Period to calculate rolling min and max's over
        smooth_period:
            Period to calculate simple moving average of %K
    Returns:
        df:
            Adjusted market dataframe
    """

    # The minimum low over the past window bars.
    lowest_low = df[ "low" ].rolling( window=period ).min()
    # The largest high over the past window bars.
    highest_high = df[ "high" ].rolling( window=period ).max()

    # Formula for %K. E.g: >80% suggests overbought and <20% is oversold.
    df[ f"stoch_k_{period}" ] = 100 * ( (df[ "close" ] - lowest_low) / (highest_high - lowest_low) ).shift(1)

    # %D = SMA of %K over 'smooth' period --- no .shfit(1) need since %K already shifted
    df[ f"stoch_d_{period}" ] = df[ f"stoch_k_{period}" ].rolling( window=smooth_period ).mean()

    return df


def addCCI(
        df,
        period
        ):
    """
    Add Commodity Channel Index (CCI) information, a momentum based indicator.
    Larger values (+100) indicates overbought and lower (-100) values indicate
    oversold.

    Args:
        df:
            Raw market data dataframe
        period:
            Period to calculate typical price
    Returns:
        df:
            Adjusted market dataframe
    """

    # Calculate average of high, low and close
    typical_price = ( df[ "high" ] + df[ "low" ] + df[ "close" ] ) / 3

    # Calculare rolling average over 'window' for typical price (TP)
    mean_tp = typical_price.rolling( window=period ).mean()

    # Calculate Mean Absolute Deviation - how far on average each TP is from the mean of the window
    mad_tp = typical_price.rolling( window=period ).apply( lambda x: np.mean( np.abs( x-np.mean(x) ) ) )

    # Formula for CCI, 0.015 is a typical scale factor chosen to limit values -100<val<100
    df[ "cci" ] = ( (typical_price-mean_tp) / (0.015*mad_tp) ).shift( 1 )

    return df


def addVWAP(
        df,
        period
        ):
    """
    Add Volume Weighted Average Price (VWAP) information to df. Volume based
    indicator. Tells the average price paid per unit traded, weighted by
    trading volume.

    Args:
        df:
            Raw market data dataframe
        period:
            Period to calculate VWAP over
    Returns:
        df:
            Adjusted market dataframe
    """

    if "vwap_cumulative" not in df.columns:
        # Volume Weighted Average Price (VWAP)
        df[ "vwap_cumulative" ] = ( ( df[ "volume" ] * df[ "close" ] ).cumsum() / df[ "volume" ].cumsum() ).shift( 1 )

    # Calculate VWAP and guard against zeros
    denom = df[ "volume" ].rolling( period ).sum().replace( 0, np.nan )
    numer = ( df[ "volume" ] * df[ "close" ] ).rolling( period ).sum()
    df[ f"vwap_{period}" ] = ( numer / denom ).shift( 1 )

    return df


def addRollingStats(
        df,
        period
        ):
    """
    Add rolling stats for a given period.
     1.) Rolling mean    -> Trend indicator
     2.) Rolling std     -> Volatility indicator
     3.) Rolling min/max -> Support for extremes

     Args:
        df:
            Raw market data dataframe
        period:
            Period to calculate rolling stats over
    Returns:
        df:
            Adjusted market dataframe
    """

    # Calculate rolling values
    rolling = df[ "close" ].rolling( window=period )

    # Rolling mean/std/min/max, all shifted
    df[ f"rolling_mean_{period}" ] = rolling.mean().shift( 1 )
    df[ f"rolling_std_{period}" ]  = rolling.std().shift( 1 )
    df[ f"rolling_min_{period}" ]  = rolling.min().shift( 1 )
    df[ f"rolling_max_{period}" ]  = rolling.max().shift( 1 )

    return df


def addZScore(
        df,
        period
        ):
    """
    Add Z Score information. A Statistical/volatility indicator
     - Z-score > 0   -> price above rolling average
	 - Z-score < 0   -> price below rolling average
	 - |Z-score| > 2 -> unusually far from mean
                        (potential overbought/oversold depending on context)

    Args:
        df:
            Raw market data dataframe
        period:
            Period to calculate z score over
    Returns:
        df:
            Adjusted market dataframe
    """

    # Get rolling values
    rolling = df[ "close" ].rolling( window=period )

    # Calculate rolling std for defined period
    std  = rolling.std()
    mean = rolling.mean()

    if std == 0:
        raise ValueError( f"(INDICATOR) addZScore: Rolling std = 0" )
    elif mean == 0:
        raise ValueError( f"(INDICATOR) addZScore: Rolling mean = 0" )

    # Formula to calculate Z score for the given period
    df[ f"zscore_{period}" ] = ( (df[ "close" ] - mean) / std ).shift( 1 )

    return df


def addLaggedReturn(
        df,
        period
        ):
    """
    Add lagged return information.
    E.g. If period=5 -> laggedreturn_5 = ( close[t] - close[t-5] ) / close[t-5].

    Args:
        df:
            Raw market data dataframe
        period:
            Period to calculate lagged return over
    Returns:
        df:
            Adjusted market dataframe
    """
    
    # Computes pct change in closing price compared to 'period' steps earlier.
    df[ f"return_lag_{period}" ] = df[ "close" ].pct_change( periods=period ).shift( 1 )

    return df