"""
" Script to fetch and build raw market dataframe
"
" @author: Michael Kane
" @date:   09/09/2025
"""
import argparse, sys
from sagitta.client import (
    fetch_client,
    fetch_market
)
from sagitta.prep import (
    klines_to_dataframe
)
from sagitta.utils import (
    io
)


# Pass as function
def main():

    # Create parser
    parser = argparse.ArgumentParser( description="Fetch crypto market data." )

    # Define arguments
    parser.add_argument( "--keys_path", required=True, help="Path to API keys JSON"      )
    parser.add_argument( "--save_name", required=True, help="File save name"             )
    parser.add_argument( "--pair",      required=True, help="Trading pair, e.g. ETHUSDT" )
    parser.add_argument( "--period",    required=True, help="Candle period, e.g. 1h"     )
    parser.add_argument( "--start",     required=True, help="Lookback, e.g. 720d"        )

    # Get argument reference
    args   = parser.parse_args()

    # Load json holding them
    keys   = io.load_JSON( args.keys_path )[ 'test_keys' ]

    # Get public and secret keys for Testnet
    public = keys['public']
    secret = keys['secret']

    # Retrieve clients from binance
    # NOTE: These are hard configured for test account only at the moment
    main_client = fetch_client.fetchClient( clientType='main', public=public, secret=secret )

    # With client, get market information for defined pair (list output)
    klines      = fetch_market.fetchKlineData( client=main_client, pair=args.pair, period=args.period, start=args.start )

    # Convery raw klines list to a pandas dataframe
    klines_df   = klines_to_dataframe.convertKlinesToDataframe( klines )

    # As CSV
    io.save_DfToCsv( klines_df, args.save_name )

    # As parquet
    io.save_DfToParquet( klines_df, args.save_name )


# Return exit code posrt execute it
if __name__ == "__main__":
    sys.exit( main() )