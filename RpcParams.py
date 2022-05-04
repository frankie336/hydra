



class BitcoinRpcParams():
    """
    Bitcoind RPC Calls:
    https://developer.bitcoin.org/reference/rpc/
    """

    TEST_RPC_PORT = 18332
    LIVE_RPC_PORT = 8332

    def __init__(self, tx_id):
        self.tx_id  = tx_id
        self. btc_tx_params = [self.tx_id,True]


    def raw_tx_params(self):

        params = [self.tx_id,True],




