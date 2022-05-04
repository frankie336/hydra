import csv


from sqlalchemy import create_engine, MetaData, Table
import sqlalchemy
from sqlalchemy import create_engine
import pymysql#A pyhton mysql databse
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import Sequence
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select

from sqlalchemy.dialects.mysql import \
        BIGINT, BINARY, BIT, BLOB, BOOLEAN, CHAR, DATE, \
        DATETIME, DECIMAL, DECIMAL, DOUBLE, ENUM, FLOAT, INTEGER, \
        LONGBLOB, LONGTEXT, MEDIUMBLOB, MEDIUMINT, MEDIUMTEXT, NCHAR, \
        NUMERIC, NVARCHAR, REAL, SET, SMALLINT, TEXT, TIME, TIMESTAMP, \
        TINYBLOB, TINYINT, TINYTEXT, VARBINARY, VARCHAR, YEAR
pymysql.install_as_MySQLdb()


Base = declarative_base()

engine = create_engine("mysql://abe:Abe2262001$@127.0.0.1/abe",echo = True,encoding='utf8', convert_unicode=True)



""""
mysql> describe txoutxt_detail;
+--------------------+---------------+------+-----+---------+-------+
| Field              | Type          | Null | Key | Default | Extra |
+--------------------+---------------+------+-----+---------+-------+
| chain_id           | decimal(10,0) | NO   |     | NULL    |       |
| in_longest         | decimal(1,0)  | YES  |     | NULL    |       |
| block_id           | decimal(14,0) | NO   |     | NULL    |       |
| block_hash         | binary(32)    | NO   |     | NULL    |       |
| block_height       | decimal(14,0) | YES  |     | NULL    |       |
| tx_pos             | decimal(10,0) | NO   |     | NULL    |       |
| tx_id              | decimal(26,0) | NO   |     | NULL    |       |
| tx_hash            | binary(32)    | NO   |     | NULL    |       |
| tx_lockTime        | decimal(10,0) | YES  |     | NULL    |       |
| tx_version         | decimal(10,0) | YES  |     | NULL    |       |
| tx_size            | decimal(10,0) | YES  |     | NULL    |       |
| txout_id           | decimal(26,0) | NO   |     | NULL    |       |
| txout_pos          | decimal(10,0) | NO   |     | NULL    |       |
| txout_value        | decimal(30,0) | NO   |     | NULL    |       |
| txout_scriptPubKey | mediumblob    | YES  |     | NULL    |       |
| pubkey_id          | decimal(26,0) | YES  |     | NULL    |       |
| pubkey_hash        | binary(20)    | YES  |     | NULL    |       |
| pubkey             | varbinary(65) | YES  |     | NULL    |       |
+--------------------+---------------+------+-----+---------+-------+
"""

class Txout_Detail(Base):
    __tablename__ = 'txout_detail'
    chain_id = Column(DECIMAL(10,20), primary_key=True)
    in_longest = Column(DECIMAL(1,0))
    block_id = Column(DECIMAL(14,0))
    block_hash = Column(BINARY())
    block_height = Column(DECIMAL(14,0))
    tx_pos = Column(DECIMAL(10,0))
    tx_id = Column(DECIMAL(26,0))
    tx_hash = Column(BINARY(32))
    tx_lockTime = Column(DECIMAL(10,0))
    tx_version = Column(DECIMAL(10,0))
    tx_size = Column(DECIMAL(10,10))
    txout_id = Column(DECIMAL(26,0))
    txout_pos = Column(DECIMAL(10,0))
    txout_value = Column(DECIMAL(10,0))
    txout_scriptPubKey = Column(MEDIUMBLOB())
    pubkey_id = Column(DECIMAL(26,0))
    pubkey_hash = Column(BINARY(20))
    pubkey = Column(VARBINARY(65))


Session = sessionmaker(bind=engine)
session = Session()





class Queries():
    """
    This class is the parent class for queries
    """
    def __init__(self):

        self.fields = ['chain_id', 'in_longest', 'block_id', 'block_hash',
                       'block_height', 'tx_pos', 'tx_id','tx_hash',
                       'tx_lockTime', 'tx_version', 'tx_size', 'txout_id',
                       'txout_pos', 'txout_value','txout_scriptPubKey',
                       'pubkey_id', 'pubkey_hash', 'pubkey']




    def query_txout_detail(self,limit):

        row_list = []

        conn = engine.connect()

        s = select(Txout_Detail).limit(limit)

        result = conn.execute(s)

        for row in result:
            row_as_dict = row._mapping
            row_list.append(row_as_dict)

        return row_list





        #txoutxt_detail_dict = session.execute(select(Txout_Detail.chain_id, Txout_Detail.in_longest,
                                               #Txout_Detail.block_id, Txout_Detail.block_hash,
                                               #Txout_Detail.block_height, Txout_Detail.tx_pos,
                                               #Txout_Detail.tx_id, Txout_Detail.tx_hash,
                                               #Txout_Detail.tx_lockTime, Txout_Detail.tx_version,
                                               #Txout_Detail.tx_size, Txout_Detail.txout_id,
                                               #Txout_Detail.txout_pos, Txout_Detail.txout_value,
                                               #Txout_Detail.txout_scriptPubKey, Txout_Detail.pubkey_id,
                                               #Txout_Detail.pubkey_hash,
                                               #Txout_Detail.pubkey)).mappings().all()

        #return txoutxt_detail_dict


    def norm(self):

        """
        Call the list of dictionaries generated by the
        orm select operation on the sql table.
        """
        txoutxt_detail_dict = self.query_txout_detail(limit=400)

        block_hash_norm,tx_hash_norm,pubkey_hash_norm,pubkey_norm = [],[],[],[]

        for index in range(len(txoutxt_detail_dict)):
            for key in  txoutxt_detail_dict[index]:
                if key == 'block_hash':
                    try:
                        block_hash = txoutxt_detail_dict[index][key].hex()
                        block_hash_norm.append(block_hash)
                    except (AttributeError):
                        block_hash_norm.append('None Found!')


                if key == 'tx_hash':
                    try:
                        tx_hash = txoutxt_detail_dict[index][key].hex()
                        tx_hash_norm.append(tx_hash)
                    except (AttributeError):
                        tx_hash_norm.append('None Found!')


                if key == 'pubkey_hash':
                    try:
                        pubkey_hash = txoutxt_detail_dict[index][key].hex()
                        pubkey_hash_norm.append(pubkey_hash)
                    except (AttributeError):
                        pubkey_hash_norm.append('None Found!')

                if key == 'pubkey':

                    try:
                        pubkey = txoutxt_detail_dict[index][key].hex()
                        pubkey_norm.append(pubkey_hash)
                    except (AttributeError):
                        pubkey_norm.append('None Found!')

        #print(tx_hash_norm)


    def create_new_dict(self):




        byte_columns = ['block_hash','tx_hash','pubkey_hash',
                        'pubkey','txout_scriptPubKey']

        new_dict0 = {'chain_id': 0, 'in_longest': 0, 'block_id': 0, 'block_hash': 0,
                     'block_height': 0, 'tx_pos': 0,'tx_id': 0,'tx_hash': 0,
                     'tx_lockTime': 0, 'tx_version': 0, 'tx_size': 0, 'txout_id': 0,
                     'txout_pos': 0,'txout_value': 0,'txout_scriptPubKey': 0, 'pubkey_id': 0,
                     'pubkey_hash': 0, 'pubkey': 10

                     }

        txoutxt_detail_dicts = self.query_txout_detail(limit=4000)

        txoutxt_detail_dicts_new = []

        for index in range(len(txoutxt_detail_dicts)):
            for key in txoutxt_detail_dicts[index]:
                if key in byte_columns:
                    try:
                        new_dict0[key] = txoutxt_detail_dicts[index][key].hex()
                    except (AttributeError):
                        new_dict0[key]= 'None Found'
                if key not in byte_columns:
                    new_dict0[key] = txoutxt_detail_dicts[index][key]

            txoutxt_detail_dicts_new.append(new_dict0.copy())


        return txoutxt_detail_dicts_new







    def create_csv(self,filename):



        with open(filename+'.csv', 'w') as f:
            w = csv.DictWriter(f,fieldnames=self.fields)
            w.writeheader()
            #w.writerow(new_dict1)

    def append_to_csv(self,filename):

        fields = ['chain_id', 'in_longest', 'block_id', 'block_hash', 'block_height', 'tx_pos', 'tx_id',
                  'tx_hash', 'tx_lockTime', 'tx_version', 'tx_size', 'txout_id', 'txout_pos', 'txout_value',
                  'txout_scriptPubKey', 'pubkey_id', 'pubkey_hash', 'pubkey']

        txoutxt_detail_dict_new = self.create_new_dict()



        for index in range(len(txoutxt_detail_dict_new)):
            with open(filename+'.csv', 'a') as f:
                w = csv.DictWriter(f,fieldnames=fields)
                #w.writeheader()
                w.writerow(txoutxt_detail_dict_new[index])



    def chained(self):

        """
        Create .csv file
        """
        filename ='test'
        self.create_csv(filename)
        """
        Append results to the .csv file 
        """
        self.append_to_csv(filename)





    def tx_out_detail_cols(self):

        columns  = (Txout_Detail.sechain_id, Txout_Detail.in_longest,
                    Txout_Detail.block_id, Txout_Detail.block_hash,
                    Txout_Detail.block_height, Txout_Detail.tx_pos,
                    Txout_Detail.tx_id, Txout_Detail.tx_hash,
                    Txout_Detail.tx_lockTime, Txout_Detail.tx_version,
                    Txout_Detail.tx_size, Txout_Detail.txout_id,
                    Txout_Detail.txout_pos, Txout_Detail.txout_value,
                    Txout_Detail.txout_scriptPubKey, Txout_Detail.pubkey_id,
                    Txout_Detail.pubkey_hash,Txout_Detail.pubkey)

        return columns





class QueriesBase():
    """
    This class is the parent class for queries
    """





class ColumnsBase():
    """
    This is the base class for table column names,
    and other elements common to all tables
    """
    def __init__(self):
        self.chain_id = 'chain_id'
        self.in_longest = 'in_longest'
        self.block_id = 'block_id'
        self.block_hash = 'block_hash'
        self.block_height = 'block_height '
        self.tx_pos = 'tx_pos'
        self.tx_id = 'tx_id '
        self.tx_hash = 'tx_hash'
        self.tx_lockTime = 'tx_lockTime'
        self.tx_version = 'tx_version'
        self.tx_size = 'tx_size'
        self.txout_id = 'txout_id'
        self.txout_pos = 'txout_pos'
        self.txout_value = 'txout_value'
        self.txout_scriptPubKey = 'txout_scriptPubKey'
        self.pubkey_id = 'pubkey_id'
        self.pubkey_hash = 'pubkey_hash'
        self.pubkey = 'pubkey'
        self.txout_approx_value = 'txout_approx_value'

        self.txin_id = 'txin_id'
        self.txout_tx_hash = 'txout_tx_hash'
        self.txout_pos = 'txout_pos'

        self.block_version = 'block_version'
        self.block_hashMerkleRoot= 'block_hashMerkleRoot'
        self.block_nTime = 'block_nTime'
        self.block_nBits = 'block_nBits'
        self.block_nNonce = 'block_nNonce'
        self. prev_block_id = 'prev_block_id'
        self.search_block_id = 'search_block_id'
        self.block_chain_work = 'block_chain_work'
        self.block_value_in = 'block_value_in'
        self.block_value_out = 'block_value_out'
        self.block_total_satoshis = 'block_total_satoshis'
        self.block_total_seconds = 'block_total_seconds'
        self.block_satoshi_seconds = 'block_satoshi_seconds'
        self.block_total_ss = 'block_total_ss'
        self.block_num_tx = 'block_num_tx'
        self.block_ss_destroyed = 'block_ss_destroyed'

        self.next_block_id = 'next_block_id'

        self.out_block_id ='out_block_id'

        self.chain_id = 'chain_id'
        self.chain_name = 'chain_name'
        self.chain_code3 = 'chain_code3'
        self.chain_address_version = 'chain_address_version'
        self.chain_script_addr_vers = 'chain_script_addr_vers'
        self.chain_magic = 'chain_magic'
        self.chain_policy = 'chain_policy'
        self.chain_decimals = 'chain_decimals'
        self.chain_last_block_id = 'chain_last_block_id'

        self.block_num_tx = 'block_num_tx'

        self.multisig_id = 'multisig_id'

        self.block_hashPrev = 'block_hashPrev'

        self.txin_scriptSig = 'txin_scriptSig'
        self.txin_sequence = 'txin_sequence'

        self.txin_pos = 'txin_pos'
        self.prevout_id = 'prevout_id'
        self.txin_value  = 'txin_value '
        self.txin_scriptPubKey  = 'txin_scriptPubKey'






"""
class Columns(ColumnsBase):
    def __init__(self):
        super().__init__()
        pass
"""


















""""
query_output_dict = session.execute(select(Txout_Detail.chain_id,Txout_Detail.in_longest,
                              Txout_Detail.block_id,Txout_Detail.block_hash,
                              Txout_Detail.block_height,Txout_Detail.tx_pos,
                              Txout_Detail.tx_id,Txout_Detail.tx_hash,
                              Txout_Detail.tx_lockTime,Txout_Detail.tx_version,
                              Txout_Detail.tx_size,Txout_Detail.txout_id,
                              Txout_Detail.txout_pos,Txout_Detail.txout_value,
                              Txout_Detail.txout_scriptPubKey,Txout_Detail.pubkey_id,
                              Txout_Detail.pubkey_hash,
                              Txout_Detail.pubkey)).mappings().all()






for index in range(len(query_output_dict)):
    for key in query_output_dict[index]:
        print(query_output_dict[index][key])



"""



#for key, value in xdict.test() :
    #print(key, value)




    #if  key == 'block_hash':
        #xdict['block_hash'] = value.hex()

        #print(key, value.hex())


    #cols.append(key)







#print(cols)
import pandas as pd
"""
test_df = pd.DataFrame.from_dict(test)
print(test_df['block_hash'])


with open('test4.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=cols)
    writer.writeheader()
    writer.writerows(test)



enc = pd.read_csv('test4.csv',encoding='latin-1')


print(enc)
"""
a = Queries()
a.chained()
