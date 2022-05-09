





class Credentials():

    def __init__(self):
        """
        Bitcoind rpc credentials
        """
        self.__rpc_user = "frankie336"
        self.__rpc_pass = "vallois3362001$"
        self.__abe_mysql_user = "abe"
        self.__abe_mysql_pass = "Abe2262001$"


    def btcd_rpc_user(self):

        return self.__rpc_user

    def btcd_rpc_pass(self):

        return self.__rpc_pass

    def abe_mysql_user(self):

        return self.__abe_mysql_user

    def abe_mysql_pass(self):

        return self.__abe_mysql_pass

