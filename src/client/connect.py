"""
" Functionality to connect to Binance Testnet
"
" @author: Michael Kane
" @date:   07/09/2025
"""
from binance.client import Client
import utils.io as io
    

def getClient(
        keysPath,
        clientType
        ):
    """
    Get binance main or test client

    Args:
        keysPath:
            Path to API keys
        clientType:
            Test or Main client
    Returns:
        client:
            Binance client
    """

    # Load keys
    keys = io.loadJSON( keysPath )

    # Fetch client
    client = Client( api_key=keys["TestKeys"]["Test_API_Key"],
                     api_secret=keys["TestKeys"]["Test_Secret_Key"] )

    # Configure API URL depending on client type
    if clientType == "test":
        client.API_URL = "https://testnet.binance.vision/api"
    elif clientType == "main":
        client.API_URL = "https://api.binance.com/api"
    else:
        raise ValueError( f"Invalid client type {clientType}" )

    return client