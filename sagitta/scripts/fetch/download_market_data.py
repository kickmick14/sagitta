"""
" Script to fetch and build market kline dataframe
"
" @author: Michael Kane
" @date:   09/09/2025
"""
import pandas as pd
from datetime import date
from client import (
    fetch_client,
    fetch_market
)
from prep import (
    klines_to_dataframe
)
from utils import (
    io
)


# Set paths
path_to_keys   = 'secrets/keys.json'
path_to_output = 'data/raw'

# Load json holding them
test_keys   = io.loadJSON( path_to_keys )['test_keys']

# Get public and secret keys for Testnet
test_public = test_keys['public']
test_secret = test_keys['secret']

# Retrieve clients from binance
test_client = fetch_client.fetchClient( clientType='test', public=test_public, secret=test_secret )
main_client = fetch_client.fetchClient( clientType='main', public=test_public, secret=test_secret )

# Define market to fetch
pair        = 'ETHUSDT'
period      = '1h'
start       = '720d'

# Get data for save name
today       = date.today()
save_name   = f'{pair}_{period}_{start}_{today}'

# With client, get market information for defined pair (list output)
klines      = fetch_market.fetchKlineData( client=main_client, pair=pair, period=period, start=start )

# Convery raw klines list to a pandas dataframe
klines_df   = klines_to_dataframe.convertKlinesToDataframe( klines )

# Save dataframes: parquet for later loading and csv for personal viewing and portability
path        = f'{path_to_output}/{save_name}'
# As CSV
io.dfToCsv( klines_df, path )
# As parquet
io.dfToParquet( klines_df, path )