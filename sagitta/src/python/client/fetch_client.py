"""
" Functionality to connect to Binance Testnet
"
" @author: Michael Kane
" @date:   07/09/2025
"""
from binance.client import Client
import utils.io as io
    

def fetchClient(
        clientType,
        public,
        secret
        ):
    """
    Get binance main or test client

    Args:
        clientType:
            Test or main client
        public:
            Public key to Testnet
        secret:
            Secret key to Testnet
    Returns:
        client:
            Binance client
    """

    print( " (CLIENT) Retrieving client... ")

    # Fetch client
    client = Client(
        api_key=public,
        api_secret=secret
        )

    # Configure API URL depending on client type
    if clientType == "test":
        client.API_URL = "https://testnet.binance.vision/api"
    elif clientType == "main":
        client.API_URL = "https://api.binance.com/api"
    else:
        raise ValueError( f"Invalid client type {clientType}" )
    
    print(f" (CLIENT) Retrieved clientType={clientType}... ", client)

    return client