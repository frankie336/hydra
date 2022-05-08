"""
local custom imports
"""
from Credentials import Credentials
"""
General package imports
"""
import csv
import math
from math import floor
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

engine = create_engine("mysql://abe:Abe2262001$@127.0.0.1/abe",echo = False,encoding='utf8')




"""

mysql> describe block;
+-----------------------+---------------+------+-----+---------+-------+
| Field                 | Type          | Null | Key | Default | Extra |
+-----------------------+---------------+------+-----+---------+-------+
| block_id              | decimal(14,0) | NO   | PRI | NULL    |       |
| block_hash            | binary(32)    | NO   | UNI | NULL    |       |
| block_version         | decimal(10,0) | YES  |     | NULL    |       |
| block_hashMerkleRoot  | binary(32)    | YES  |     | NULL    |       |
| block_nTime           | decimal(20,0) | YES  |     | NULL    |       |
| block_nBits           | decimal(10,0) | YES  |     | NULL    |       |
| block_nNonce          | decimal(10,0) | YES  |     | NULL    |       |
| block_height          | decimal(14,0) | YES  |     | NULL    |       |
| prev_block_id         | decimal(14,0) | YES  | MUL | NULL    |       |
| search_block_id       | decimal(14,0) | YES  | MUL | NULL    |       |
| block_chain_work      | binary(38)    | YES  |     | NULL    |       |
| block_value_in        | decimal(30,0) | YES  |     | NULL    |       |
| block_value_out       | decimal(30,0) | YES  |     | NULL    |       |
| block_total_satoshis  | decimal(26,0) | YES  |     | NULL    |       |
| block_total_seconds   | decimal(20,0) | YES  |     | NULL    |       |
| block_satoshi_seconds | decimal(28,0) | YES  |     | NULL    |       |
| block_total_ss        | decimal(28,0) | YES  |     | NULL    |       |
| block_num_tx          | decimal(10,0) | NO   |     | NULL    |       |
| block_ss_destroyed    | decimal(28,0) | YES  |     | NULL    |       |
+-----------------------+---------------+------+-----+---------+-------+

"""

class BlockTable(Base):
    __tablename__ = 'block'
    block_id = Column(DECIMAL(14,0),primary_key=True)
    block_hash = Column(BINARY(32))
    block_version = Column(DECIMAL(10,0))
    block_hashMerkleRoot = Column(BINARY(32))
    block_nTime = Column(DECIMAL(20,0))
    block_nBits = Column(DECIMAL(10,0))
    block_nNonce = Column(DECIMAL(10,0))
    block_height = Column(DECIMAL(14,0))
    prev_block_id = Column(DECIMAL(14,0))
    search_block_id = Column(DECIMAL(14,0))
    block_chain_work = Column(BINARY(38))
    block_value_in = Column(DECIMAL(30,0))
    block_value_out = Column(DECIMAL(30,0))
    block_total_satoshis = Column(DECIMAL(26,0))
    block_total_seconds = Column(DECIMAL(20,0))
    block_satoshi_seconds = Column(DECIMAL(28,0))
    block_total_ss = Column(DECIMAL(28,0))
    block_num_tx = Column(DECIMAL(10,0))
    block_ss_destroyed = Column(DECIMAL(28,0))





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

class TxoutDetailTable(Base):
    __tablename__ = 'txout_detail'
    chain_id = Column(DECIMAL(10,20), primary_key=True)
    in_longest = Column(DECIMAL(1,0))
    block_id = Column(DECIMAL(14,0))
    block_hash = Column(BINARY(32))
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


"""
mysql> describe txin_detail;
+-------------------+---------------+------+-----+---------+-------+
| Field             | Type          | Null | Key | Default | Extra |
+-------------------+---------------+------+-----+---------+-------+
| chain_id          | decimal(10,0) | NO   |     | NULL    |       |
| in_longest        | decimal(1,0)  | YES  |     | NULL    |       |
| block_id          | decimal(14,0) | NO   |     | NULL    |       |
| block_hash        | binary(32)    | NO   |     | NULL    |       |
| block_height      | decimal(14,0) | YES  |     | NULL    |       |
| tx_pos            | decimal(10,0) | NO   |     | NULL    |       |
| tx_id             | decimal(26,0) | NO   |     | NULL    |       |
| tx_hash           | binary(32)    | NO   |     | NULL    |       |
| tx_lockTime       | decimal(10,0) | YES  |     | NULL    |       |
| tx_version        | decimal(10,0) | YES  |     | NULL    |       |
| tx_size           | decimal(10,0) | YES  |     | NULL    |       |
| txin_id           | decimal(26,0) | NO   |     | NULL    |       |
| txin_pos          | decimal(10,0) | NO   |     | NULL    |       |
| prevout_id        | decimal(26,0) | YES  |     | NULL    |       |
| txin_scriptSig    | mediumblob    | YES  |     | NULL    |       |
| txin_sequence     | decimal(10,0) | YES  |     | NULL    |       |
| txin_value        | decimal(30,0) | YES  |     | NULL    |       |
| txin_scriptPubKey | mediumblob    | YES  |     | NULL    |       |
| pubkey_id         | decimal(26,0) | YES  |     | NULL    |       |
| pubkey_hash       | binary(20)    | YES  |     | NULL    |       |
| pubkey            | varbinary(65) | YES  |     | NULL    |       |
+-------------------+---------------+------+-----+---------+-------+

"""
class TxinDetailTable(Base):
    __tablename__ = 'txin_detail'
    chain_id = Column(DECIMAL(10,20), primary_key=True)
    in_longest = Column(DECIMAL(1,0))
    block_id = Column(DECIMAL(14,0))
    block_hash = Column(BINARY(32))
    block_height = Column(DECIMAL(14,0))
    tx_pos = Column(DECIMAL(10,0))
    tx_id = Column(DECIMAL(26,0))
    tx_hash = Column(BINARY(32))
    tx_lockTime = Column(DECIMAL(10,0))
    tx_version = Column(DECIMAL(10,0))
    tx_size = Column(DECIMAL(10,10))
    txin_id = Column(DECIMAL(26,0))
    txin_pos = Column(DECIMAL(10,0))
    prevout_id = Column(DECIMAL(26,0))
    txin_scriptSig = Column(MEDIUMBLOB())
    txin_sequence = Column(DECIMAL(10,0))
    txin_value = Column(DECIMAL(30,0))
    txin_scriptPubKey = Column(MEDIUMBLOB())
    pubkey_id = Column(DECIMAL(26,0))
    pubkey_hash = Column(BINARY(20))
    pubkey = Column(VARBINARY(65))





Session = sessionmaker(bind=engine)
session = Session()

class QueriesBase():
    """
    This class is the parent class for queries
    """
    def __init__(self,rows,percent,offset,floor_div,
                 loop_floor_range,remainder,chunks,
                 clean_dict):

        self.txout_detail_fields = ['chain_id', 'in_longest', 'block_id', 'block_hash',
                       'block_height', 'tx_pos', 'tx_id','tx_hash',
                       'tx_lockTime', 'tx_version', 'tx_size', 'txout_id',
                       'txout_pos', 'txout_value','txout_scriptPubKey',
                       'pubkey_id', 'pubkey_hash', 'pubkey'
                                    ]



        self.txin_detail_fields = ['chain_id', 'in_longest', 'block_id', 'block_hash',
                                    'block_height', 'tx_pos', 'tx_id', 'tx_hash',
                                    'tx_lockTime', 'tx_version', 'tx_size', 'txin_id',
                                    'txin_pos', 'prevout_id', 'txin_scriptSig',
                                    'txin_sequence', 'txin_value', 'txin_scriptPubKey',
                                   'pubkey_id','pubkey_hash','pubkey'
                                   ]


        self.block_fields = ['block_id','block_hash','block_version',
                             'block_hashMerkleRoot','block_nTime','block_nBits','block_nNonce',
                             'block_height','prev_block_id','search_block_id','block_chain_work',
                             'block_value_in','block_value_out','block_total_satoshis','block_total_seconds',
                             'block_satoshi_seconds','block_total_ss','block_num_tx','block_ss_destroyed'

                             ]




        self.__total_rows = rows
        self.__rows_percent = percent
        self.__offset = offset
        self.__floor_div = floor_div
        self.__loop_floor_range = loop_floor_range
        self.__remainder = remainder
        self.__chunks = chunks
        self.__clean_dict = clean_dict

    def set_rows(self, rows):
        self.__total_rows = rows

    def get_rows(self):
        return self.__total_rows

    def set_percent(self, percent):
        self.__rows_percent = percent

    def get_percent(self):
        return self.__rows_percent

    def set_offset(self, offset):
        self.__offset = offset

    def get_offset(self):
        return self.__offset

    def set_floor_div(self, floor_div):
        self.__floor_div = floor_div

    def get_floor_div(self):
        return self.__floor_div

    def set_loop_floor_range(self,loop_floor_range):
        self.__loop_floor_range = loop_floor_range

    def get_loop_floor_range(self):
        return self.__loop_floor_range


    def set_remainder(self,remainder):
        self.__remainder = remainder

    def get_remainder(self, remainder):
        return self.__remainder

    def set_chunks(self,chunks):
        self.__chunks = chunks

    def get_chunks(self, chunks):
        return self.__chunks

    def set_clean_dict(self, clean_dict):
        self.__clean_dict = clean_dict

    def get_clean_dict(self, chunks):
        return self.__clean_dict





    def query_total_rows(self, table):

        rows_number = session.query(table).count()

        return rows_number

    def floor_division(self, a, b):

        floor_x = floor(a / b)
        remainder_y = (a % b)

        return floor_x, remainder_y

    def in_chunks(self, percentage):

        total_rows = self.__total_rows

        percent = total_rows/100*percentage
        rounded = math.ceil(percent)

        return  rounded



    def set_initial_size(self,filename):

        row_per = self.in_chunks(percentage=self.__rows_percent)
        current_numb = self.read_tracker_file(folder=_dir + '/tracking', filename=filename)

        if current_numb > row_per:

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

        return int(firstline)




    def query_table(self,table,limit,offset,file_name):

        """
        1. Queries the table
        """
        row_per = self.in_chunks(percentage=self.__rows_percent)
        self.write_files(folder=_dir + '/tracking', filename='/txout_detail_start_pos', last_number=str(row_per))

        row_list = []

        conn = engine.connect()
        s = select(table).limit(limit).offset(offset)
        result = conn.execute(s)

        for row in result:
            row_as_dict = row._mapping
            row_list.append(row_as_dict)


        """
        Update the Tracker file here 
        """
        self.track_index(filename=file_name)

        return row_list


    def track_index(self,filename):
        """
        Update the Tracker file here
        """
        row_per = self.in_chunks(percentage=1)
        current_numb = self.read_tracker_file(folder=_dir + '/tracking', filename=filename)
        start_pos = self.in_chunks(percentage=self.__rows_percent)
        new_numb = current_numb + row_per

        if current_numb == 0:
            change_num = start_pos
            self.write_files(folder=_dir + '/tracking', filename=filename, last_number=str(change_num))
        else:
            pass

        if current_numb == start_pos:
            new_numb = current_numb + row_per
            change_num = new_numb
            self.write_files(folder=_dir + '/tracking', filename=filename, last_number=str(change_num))
        else:
            pass

        if current_numb > start_pos:
            new_numb = current_numb + row_per
            change_num = new_numb
            self.write_files(folder=_dir + '/tracking', filename=filename, last_number=str(change_num))



    def set_offset(self,filename):

        rows_percent = self.in_chunks(percentage=self.__rows_percent)
        current_numb = self.read_tracker_file(folder=_dir + '/tracking', filename=filename)

        if current_numb == rows_percent:
            offset_to = 0
        else:
            offset_to = self.read_tracker_file(folder=_dir + '/tracking', filename=filename)

        return offset_to




    def create_new_dict(self,offset,table,clean_dict,filename):
        """
        Some fields from the abe Mysql tables have assigned data types as C++ x0 bytes.
        Python reads these encoded,example: '\x04\x04\xfbJ\xf1\x16\x98X\x8f\x00\ ...'.
        Usually Bitcoin hash values. Need to be converted to .hex values, example:
        '4a5e1e4baab89f3a3....'

        -Identifies encoded byte columns from query results
        - Converts to .hex
        """
        byte_columns = ['block_hash','tx_hash','pubkey_hash','pubkey',
                        'txout_scriptPubKey','txin_scriptSig','block_hashMerkleRoot','block_chain_work',


                        ]


        rows_percent = self.in_chunks(percentage=self.__rows_percent)

        txoutxt_detail_dicts = self.query_table(table=table,limit=rows_percent,offset=offset,file_name=filename)

        txoutxt_detail_dicts_new = []

        for index in range(len(txoutxt_detail_dicts)):
            for key in txoutxt_detail_dicts[index]:
                if key in byte_columns:
                    try:
                        clean_dict[key] = txoutxt_detail_dicts[index][key].hex()
                    except (AttributeError):
                        clean_dict[key]= 'None Found'
                if key not in byte_columns:
                    clean_dict[key] = txoutxt_detail_dicts[index][key]

            txoutxt_detail_dicts_new.append(clean_dict.copy())

        return txoutxt_detail_dicts_new




    def create_base_csv(self,folder,filename,field_names):
        """
        Creates .csv file with base headers
        """
        file_exists = os.path.exists(folder + '/' + filename + '.csv')
        if file_exists == True:
            return


        with open(folder + '/' + filename + '.csv', 'w') as f:
            w = csv.DictWriter(f, fieldnames=field_names)
            w.writeheader()
            #w.writerow(new_dict1)

    def append_to_csv(self,table,fields,folder,filename,offset,track_file):
        """
        Appends new data to .csv
        """
        txoutxt_detail_dict_new = self.create_new_dict(offset,table=table,clean_dict=self.__clean_dict,
                                                       filename=track_file)

        for index in range(len(txoutxt_detail_dict_new)):
            with open(folder + '/' + filename + '.csv', 'a') as f:
                w = csv.DictWriter(f, fieldnames=fields)
                #w.writeheader()
                w.writerow(txoutxt_detail_dict_new[index])


    def set_up(self,table,percentage):
        """
        Sets the total row number object
        """
        query_total_rows = self.query_total_rows(table)
        self.set_rows(query_total_rows)
        """
        Sets the percentage of rows that should be processed on each pass
        """
        self.set_percent(percent=percentage)  # 1-set the row percentage per pass
        """
        Divide Queries into smaller chunks 
        """
        chunks = self.in_chunks(percentage=self.__rows_percent)
        self.set_chunks(chunks)

        """
        Floor division: between the number of rows processed on each pass
        and the total number of rows in the table.
        """
        floor_div = self.floor_division(a=self.__total_rows, b=chunks)
        self.set_floor_div(floor_div)

        loop_floor_range = floor_div[0]
        self.set_loop_floor_range(loop_floor_range)

        remainder = floor_div[1]
        self.set_remainder(remainder)




    def loop_logic(self,table,fields,file_name,track_file):

        floor_div = self.floor_division(a=self.__total_rows, b=self.__chunks )
        loop_floor_range = floor_div[0]
        remainder = floor_div[1]
        """
        Create .csv file and headers
        """
        for x in range(self.__loop_floor_range):
            self.create_base_csv(folder=_dir + '/output', filename=file_name,field_names=fields)
            """
            Append results to the .csv file
            """
            offset = self.set_offset(filename=file_name)
            self.append_to_csv(table=table,fields=fields,folder=_dir + '/output', filename=file_name,
                               offset=offset,track_file=track_file)

        if self.__remainder == 0:
            return
        else:
            print('There is a remainder following the floor')
            offset = self.__total_rows - remainder
            self.append_to_csv(table=table,fields=fields,folder = _dir + '/output', filename =file_name,
                               offset=offset,track_file=track_file)


        print('The chunk is:',self.__chunks, '\n',
              'The loop floor is:', self.__loop_floor_range,'\n',
              'The remainder is:', remainder)







class BlockQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = {'block_id': 0, 'block_hash': 0, 'block_version': 0, 'block_hashMerkleRoot': 0,
                      'block_nTime': 0, 'block_nBits': 0, 'block_nNonce': 0, 'block_height': 0,
                      'prev_block_id': 0, 'search_block_id': 0, 'block_chain_work': 0, 'block_value_in': 0,
                      'block_value_out': 0, 'block_total_satoshis': 0, 'block_total_seconds': 0, 'block_satoshi_seconds': 0,
                      'block_total_ss': 0, 'block_num_tx': 0, 'block_ss_destroyed': 0,

                      }


    def block_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=BlockTable, percentage=88)
        """
        Looping
        """
        self.loop_logic(table=BlockTable, fields=self.block_fields, file_name='block',
                        track_file='/block')








class TxinQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


    def txin_detail_flow(self):
        """
        Clean dict
        """
        clean_dict = {'chain_id': 0, 'in_longest': 0, 'block_id': 0, 'block_hash': 0,
                      'block_height': 0, 'tx_pos': 0, 'tx_id': 0, 'tx_hash': 0,
                      'tx_lockTime': 0, 'tx_version': 0, 'tx_size': 0, 'txin_id': 0,
                      'txin_pos': 0, 'prevout_id': 0, 'txin_scriptSig': 0, 'txin_sequence': 0,
                      'txin_value': 0, 'txin_scriptPubKey': 0, 'pubkey_id': 0, 'pubkey_hash': 0,
                      'pubkey': 0

                      }
        self.set_clean_dict(clean_dict)


        """
        Setting up
        """
        self.set_up(table=TxinDetailTable, percentage=88)

        """
        Looping
        """
        self.loop_logic(table=TxinDetailTable, fields=self.txin_detail_fields, file_name='txin_detail',
                        track_file='/txin_detail')



class TxoutQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                     loop_floor_range, remainder, chunks,
                     clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                             loop_floor_range, remainder, chunks,
                             clean_dict)


    def txout_detail_flow(self):

        clean_dict = {'chain_id': 0, 'in_longest': 0, 'block_id': 0, 'block_hash': 0,
                         'block_height': 0, 'tx_pos': 0, 'tx_id': 0, 'tx_hash': 0,
                         'tx_lockTime': 0, 'tx_version': 0, 'tx_size': 0, 'txout_id': 0,
                         'txout_pos': 0, 'txout_value': 0, 'txout_scriptPubKey': 0, 'pubkey_id': 0,
                         'pubkey_hash': 0, 'pubkey': 0

                         }

        self.set_clean_dict(clean_dict)
        """
        Setting up
        """
        self.set_up(table=TxoutDetailTable, percentage=88)
        """
        Looping
        """
        self.loop_logic(table=TxoutDetailTable, fields=self.txout_detail_fields, file_name='txout_detail',
                        track_file='/txout_detail')



def main():
    """
    Creates .csv for txout_detail
    """

    """
    creates .csv for txin_detail
    """
    #a = QueriesBase(rows=0, percent=0, offset=0, floor_div=0,
                    #loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)

    b = TxinQuery(rows=0, percent=0, offset=0, floor_div=0,
                  loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    b.txin_detail_flow()

    c = TxoutQuery(rows=0, percent=0, offset=0, floor_div=0,
                  loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    c.txout_detail_flow()


    d = BlockQuery(rows=0, percent=0, offset=0, floor_div=0,
                   loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    d.block_flow()


main()