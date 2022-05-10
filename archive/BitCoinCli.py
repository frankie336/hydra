from __future__ import print_function
import requests, json
"""local"""
from Rpc import BitcoinRpc
from RpcParams import BitcoinRpcParams
from Credentials import Credentials






class  BaseRpc():
    def __init__(self,url,_id,headers,params,payload):
        self._id = _id
        self.url = url
        self.headers = headers
        self.params = params
        self.payload = payload

    def rpc_post(self,serverURL,headers,payload):

        response = requests.post(serverURL, headers=headers, data=payload)

        jsonResponse = response.json()

        values = jsonResponse['result']

        #print(values['txid'])

        for key, value in values.items():
            print(key, ":", value)





        #return (jsonResponse)




class tx_rpc(BaseRpc):
    def __init__(self,url,_id,headers,params,payload):
        super().__init__(url,_id,headers,params,payload)


    def btc_tx_search(self,tx_id):

        tx_lookup = self.rpc_post(self.url, self.headers, self.payload)

        print(tx_lookup)


#a = BaseRpc("f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16")
#a.test2()



credentials = Credentials()
rpc_user = credentials.btcd_rpc_user()
rpc_pass = credentials.btcd_rpc_pass()


tx_id = "f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16"
url= 'http://' + rpc_user  + ':' + rpc_pass + '@localhost:' + '8332'
_id = tx_id

headers = {'content-type': 'application/json'}
params= [_id, True]
payload = json.dumps(
    {"method": BitcoinRpc.RAW_TRANSACTION, "params": params,
     "jsonrpc": "2.0"})


b =tx_rpc(url,_id,headers,params,payload)
b.btc_tx_search(tx_id)









