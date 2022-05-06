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
    def __init__(self,rows,percent,offset):

        self.txin_detail_fields = ['chain_id', 'in_longest', 'block_id', 'block_hash',
                       'block_height', 'tx_pos', 'tx_id','tx_hash',
                       'tx_lockTime', 'tx_version', 'tx_size', 'txout_id',
                       'txout_pos', 'txout_value','txout_scriptPubKey',
                       'pubkey_id', 'pubkey_hash', 'pubkey']

        self.__total_rows = rows
        self.__rows_percent = percent
        self.__offset = offset

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

            #print(firstline)

        return int(firstline)




    def query_txout_detail(self,limit,offset):

        """
        write the start positing to file 
        """
        row_per = self.in_chunks(percentage=self.__rows_percent)
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
            self.write_files(folder=_dir + '/tracking', filename='/txout_detail', last_number=str(change_num))
        else:
            pass

        if current_numb > start_pos:
            new_numb = current_numb + row_per
            change_num = new_numb
            self.write_files(folder=_dir + '/tracking', filename='/txout_detail', last_number=str(change_num))



    def set_offset(self,filename):

        rows_percent = self.in_chunks(percentage=self.__rows_percent)
        current_numb = self.read_tracker_file(folder=_dir + '/tracking', filename=filename)

        if current_numb == rows_percent:
            offset_to = 0
        else:
            offset_to = self.read_tracker_file(folder=_dir + '/tracking', filename=filename)

        return offset_to




    def create_new_dict(self,offset):
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

        rows_percent = self.in_chunks(percentage=self.__rows_percent)

        txoutxt_detail_dicts = self.query_txout_detail(limit=rows_percent,offset=offset)

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




    def create_base_csv(self, filename):
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

    def append_to_csv(self,filename,offset):
        """
        Appends new data to .csv
        """
        txoutxt_detail_dict_new = self.create_new_dict(offset)

        for index in range(len(txoutxt_detail_dict_new)):
            with open(filename+'.csv', 'a') as f:
                w = csv.DictWriter(f, fieldnames=self.txin_detail_fields)
                #w.writeheader()
                w.writerow(txoutxt_detail_dict_new[index])



    def main(self):
        """
        Sets the total row number object
        """
        query_total_rows = self.query_total_rows(Txout_Detail)
        self.set_rows(query_total_rows)
        """
        Sets the percentage of rows that should be processed on each pass
        """
        self.set_percent(percent=99)  # 1-set the row precentage per pass

        """
        """
        chunks = self.in_chunks(percentage=self.__rows_percent)
        """
        Floor division: between the number of rows processed on each pass
        and the total number of rows in the table.
        """
        floor_div = self.floor_division(a=self.__total_rows, b=chunks)
        loop_floor_range = floor_div[0]
        remainder = floor_div[1]


        for x in range(loop_floor_range):
            """
            Create .csv file and headers
            """
            filename ='test'
            self.create_base_csv(filename)
            """
            Append results to the .csv file
            """
            offset = self.set_offset(filename='/txout_detail')
            self.append_to_csv(filename,offset)


        if remainder == 0:
            return
        else:
            print('There is a remainder following the floor')
            offset = self.__total_rows - remainder
            self.append_to_csv(filename, offset)



        print('The chunk is:', chunks, '\n',
              'The loop floor is:', loop_floor_range, '\n',
              'The remainder is:', remainder)


a = Queries(rows=0,percent=0,offset=0)
a.main()
