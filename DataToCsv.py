import csv
import math
import os

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

_dir = os.getcwd()
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
    ROW_PER = 2
    def __init__(self):

        self.txin_detail_fields = ['chain_id', 'in_longest', 'block_id', 'block_hash',
                       'block_height', 'tx_pos', 'tx_id','tx_hash',
                       'tx_lockTime', 'tx_version', 'tx_size', 'txout_id',
                       'txout_pos', 'txout_value','txout_scriptPubKey',
                       'pubkey_id', 'pubkey_hash', 'pubkey']

    def count_rows(self,table):

        rows_number = session.query(table).count()

        return rows_number

    def rows_percent(self,percentage):

        rows_number = self.count_rows(Txout_Detail)

        percent = rows_number/100*percentage

        rounded = math.ceil(percent)
        print(rounded,'in chunks')

        return  rounded

        text_file = open(_dir+"/tracking/txout_detail.txt", "w")
        n = text_file.write(str(rounded))



    def set_initial_size(self,filename):

        row_per = self.rows_percent(percentage=Queries.ROW_PER)
        current_numb = self.read_tracker_file(folder=_dir + '/tracking', filename=filename)

        if current_numb > row_per:
            print('Yeah, ok')
            return
        else:
            self.write_files(folder=_dir + '/tracking', filename=filename, last_number=str(row_per))





    def write_files(self, folder, filename, last_number):

        file_exists = os.path.exists(folder+'/'+filename+'.txt')

        with open(folder+'/'+filename+'.txt', 'w') as f:
            f.write(last_number)




    def read_tracker_file(self,folder,filename):

        with open(folder+'/'+filename+'.txt') as f:
            firstline = f.readline().rstrip()

            print(firstline)

        return int(firstline)




    def query_txout_detail(self,limit,offset):

        """
        write the start positing to file 
        """
        row_per = self.rows_percent(percentage=Queries.ROW_PER)
        self.write_files(folder=_dir + '/tracking', filename='/txout_detail_start_pos', last_number=str(row_per))

        row_list = []

        conn = engine.connect()

        s = select(Txout_Detail).limit(limit).offset(offset)

        result = conn.execute(s)

        for row in result:
            row_as_dict = row._mapping
            row_list.append(row_as_dict)


        """
        Update the Tracker file here 
        """
        self.track_index(filename='/txout_detail')

        return row_list


    def track_index(self,filename):
        """
        Update the Tracker file here
        """
        row_per = self.rows_percent(percentage=1)
        current_numb = self.read_tracker_file(folder=_dir + '/tracking', filename=filename)
        start_pos = self.rows_percent(percentage=Queries.ROW_PER)
        new_numb = current_numb + row_per

        if current_numb == 0:
            change_num = start_pos
            self.write_files(folder=_dir + '/tracking', filename=filename, last_number=str(change_num))
        else:
            pass

        if current_numb == start_pos:
            new_numb = current_numb + row_per
            change_num = new_numb
            self.write_files(folder=_dir + '/tracking', filename='/txout_detail', last_number=str(change_num))
        else:
            pass

        if current_numb > start_pos:
            new_numb = current_numb + row_per
            change_num = new_numb
            self.write_files(folder=_dir + '/tracking', filename='/txout_detail', last_number=str(change_num))



    def set_offset(self,filename):

        rows_percent = self.rows_percent(percentage=Queries.ROW_PER)
        current_numb = self.read_tracker_file(folder=_dir + '/tracking', filename=filename)

        if current_numb == rows_percent:
            offset_to = 0
        else:
            offset_to = self.read_tracker_file(folder=_dir + '/tracking', filename=filename)

        return offset_to




    def create_new_dict(self):
        """
        Some fields from the abe Mysql tables have assigned data types as C++ x0 bytes.
        Python reads these encoded,example: '\x04\x04\xfbJ\xf1\x16\x98X\x8f\x00\ ...'.
        Usually Bitcoin hash values. Need to be converted to .hex values, example:
        '4a5e1e4baab89f3a3....'

        -Identifies encoded byte columns from query results
        - Converts to .hex
        """
        byte_columns = ['block_hash','tx_hash','pubkey_hash',
                        'pubkey','txout_scriptPubKey']

        rep_dict_base = {'chain_id': 0, 'in_longest': 0, 'block_id': 0, 'block_hash': 0,
                     'block_height': 0, 'tx_pos': 0,'tx_id': 0,'tx_hash': 0,
                     'tx_lockTime': 0, 'tx_version': 0, 'tx_size': 0, 'txout_id': 0,
                     'txout_pos': 0,'txout_value': 0,'txout_scriptPubKey': 0, 'pubkey_id': 0,
                     'pubkey_hash': 0, 'pubkey': 0

                     }

        rows_percent = self.rows_percent(percentage=Queries.ROW_PER)
        offset_to = self.set_offset(filename='/txout_detail')

        txoutxt_detail_dicts = self.query_txout_detail(limit=rows_percent,offset=offset_to)

        txoutxt_detail_dicts_new = []

        for index in range(len(txoutxt_detail_dicts)):
            for key in txoutxt_detail_dicts[index]:
                if key in byte_columns:
                    try:
                        rep_dict_base[key] = txoutxt_detail_dicts[index][key].hex()
                    except (AttributeError):
                        rep_dict_base[key]= 'None Found'
                if key not in byte_columns:
                    rep_dict_base[key] = txoutxt_detail_dicts[index][key]

            txoutxt_detail_dicts_new.append(rep_dict_base.copy())


        return txoutxt_detail_dicts_new





    def create_csv(self,filename):
        """
        Creates .csv file with base headers
        """

        file_exists = os.path.exists(filename + '.csv')
        if file_exists == True:
            return

        with open(filename+'.csv', 'w') as f:
            w = csv.DictWriter(f, fieldnames=self.txin_detail_fields)
            w.writeheader()
            #w.writerow(new_dict1)

    def append_to_csv(self,filename):
        """
        Appends new data to .csv
        """
        txoutxt_detail_dict_new = self.create_new_dict()

        for index in range(len(txoutxt_detail_dict_new)):
            with open(filename+'.csv', 'a') as f:
                w = csv.DictWriter(f, fieldnames=self.txin_detail_fields)
                #w.writeheader()
                w.writerow(txoutxt_detail_dict_new[index])



    def main(self):

        self.set_initial_size(filename='/txout_detail')
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



a = Queries()
a.main()
