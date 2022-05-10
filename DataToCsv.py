"""
local custom imports
"""
from Credentials import Credentials
"""
General package imports
"""
from tqdm import tqdm as tq
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

creds=Credentials()
USERNAME = creds.abe_mysql_user()
PASSWORD = creds.abe_mysql_pass()
engine = create_engine('mysql://'+USERNAME+':'+PASSWORD+'@127.0.0.1/abe',echo = False,encoding='utf8')

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


"""
mysql> describe block_next;
+---------------+---------------+------+-----+---------+-------+
| Field         | Type          | Null | Key | Default | Extra |
+---------------+---------------+------+-----+---------+-------+
| block_id      | decimal(14,0) | NO   | PRI | NULL    |       |
| next_block_id | decimal(14,0) | NO   | PRI | NULL    |       |
+---------------+---------------+------+-----+---------+-------+
"""
class BlockNextTable(Base):
    __tablename__ = 'block_next'
    block_id = Column(DECIMAL(14,0),primary_key=True)
    next_block_id =  Column(DECIMAL(14,0))


"""
mysql> describe block_seq;
+-------+--------+------+-----+---------+----------------+
| Field | Type   | Null | Key | Default | Extra          |
+-------+--------+------+-----+---------+----------------+
| id    | bigint | NO   | PRI | NULL    | auto_increment |
+-------+--------+------+-----+---------+----------------+
"""
class BlockSeqTable(Base):
    __tablename__ = 'block_seq'
    id = Column(BIGINT(),primary_key=True)

"""
mysql> describe block_tx;
+----------+---------------+------+-----+---------+-------+
| Field    | Type          | Null | Key | Default | Extra |
+----------+---------------+------+-----+---------+-------+
| block_id | decimal(14,0) | NO   | PRI | NULL    |       |
| tx_id    | decimal(26,0) | NO   | PRI | NULL    |       |
| tx_pos   | decimal(10,0) | NO   |     | NULL    |       |
+----------+---------------+------+-----+---------+-------+
"""

class BlockTxTable(Base):
    __tablename__ = 'block_tx'
    block_id = Column(DECIMAL(14,0), primary_key=True)
    tx_id = Column(DECIMAL(26, 0))
    tx_pos = Column(DECIMAL(10, 0))


"""
mysql> describe block_txin;
+--------------+---------------+------+-----+---------+-------+
| Field        | Type          | Null | Key | Default | Extra |
+--------------+---------------+------+-----+---------+-------+
| block_id     | decimal(14,0) | NO   | PRI | NULL    |       |
| txin_id      | decimal(26,0) | NO   | PRI | NULL    |       |
| out_block_id | decimal(14,0) | NO   | MUL | NULL    |       |
+--------------+---------------+------+-----+---------+-------+
"""

class BlockTxinTable(Base):
    __tablename__ = 'block_txin'
    block_id = Column(DECIMAL(14,0), primary_key=True)
    txin_id = Column(DECIMAL(26, 0))
    out_block_id = Column(DECIMAL(14, 0))

"""
mysql> describe chain;
+------------------------+----------------+------+-----+---------+-------+
| Field                  | Type           | Null | Key | Default | Extra |
+------------------------+----------------+------+-----+---------+-------+
| chain_id               | decimal(10,0)  | NO   | PRI | NULL    |       |
| chain_name             | varchar(100)   | NO   | UNI | NULL    |       |
| chain_code3            | varchar(5)     | YES  |     | NULL    |       |
| chain_address_version  | varbinary(100) | NO   |     | NULL    |       |
| chain_script_addr_vers | varbinary(100) | YES  |     | NULL    |       |
| chain_magic            | binary(4)      | YES  |     | NULL    |       |
| chain_policy           | varchar(255)   | NO   |     | NULL    |       |
| chain_decimals         | decimal(2,0)   | YES  |     | NULL    |       |
| chain_last_block_id    | decimal(14,0)  | YES  | MUL | NULL    |       |
+------------------------+----------------+------+-----+---------+-------+
"""

class ChainTable(Base):
    __tablename__ = 'chain'
    chain_id = Column(DECIMAL(10,0), primary_key=True)
    chain_name = Column(VARCHAR(100))
    chain_code3 = Column(VARCHAR(5))
    chain_address_version = Column(VARBINARY(100))
    chain_script_addr_vers = Column(VARBINARY(100))
    chain_magic = Column(BINARY(4))
    chain_policy = Column(VARCHAR(255))
    chain_decimals = Column(DECIMAL(2,0))
    chain_last_block_id = Column(DECIMAL(14,0))

"""
mysql> describe chain_candidate;
+--------------+---------------+------+-----+---------+-------+
| Field        | Type          | Null | Key | Default | Extra |
+--------------+---------------+------+-----+---------+-------+
| chain_id     | decimal(10,0) | NO   | PRI | NULL    |       |
| block_id     | decimal(14,0) | NO   | PRI | NULL    |       |
| in_longest   | decimal(1,0)  | YES  |     | NULL    |       |
| block_height | decimal(14,0) | YES  | MUL | NULL    |       |
+--------------+---------------+------+-----+---------+-------+
"""
class ChainCandidateTable(Base):
    __tablename__ = 'chain_candidate'
    chain_id = Column(DECIMAL(10,0), primary_key=True)
    block_id = Column(DECIMAL(14,0))
    in_longest = Column(DECIMAL(1,0))
    block_height = Column(DECIMAL(14,0))


"""
mysql> describe chain_seq;
+-------+--------+------+-----+---------+----------------+
| Field | Type   | Null | Key | Default | Extra          |
+-------+--------+------+-----+---------+----------------+
| id    | bigint | NO   | PRI | NULL    | auto_increment |
+-------+--------+------+-----+---------+----------------+
"""

class ChainSeqTable(Base):
    __tablename__ = 'chain_seq'
    id = Column(BIGINT(),primary_key=True)

"""
mysql> describe chain_summary;  
+-----------------------+---------------+------+-----+---------+-------+
| Field                 | Type          | Null | Key | Default | Extra |
+-----------------------+---------------+------+-----+---------+-------+
| chain_id              | decimal(10,0) | NO   |     | NULL    |       |
| in_longest            | decimal(1,0)  | YES  |     | NULL    |       |
| block_id              | decimal(14,0) | NO   |     | NULL    |       |
| block_hash            | binary(32)    | NO   |     | NULL    |       |
| block_version         | decimal(10,0) | YES  |     | NULL    |       |
| block_hashMerkleRoot  | binary(32)    | YES  |     | NULL    |       |
| block_nTime           | decimal(20,0) | YES  |     | NULL    |       |
| block_nBits           | decimal(10,0) | YES  |     | NULL    |       |
| block_nNonce          | decimal(10,0) | YES  |     | NULL    |       |
| block_height          | decimal(14,0) | YES  |     | NULL    |       |
| prev_block_id         | decimal(14,0) | YES  |     | NULL    |       |
| prev_block_hash       | binary(32)    | YES  |     | NULL    |       |
| block_chain_work      | binary(38)    | YES  |     | NULL    |       |
| block_num_tx          | decimal(10,0) | NO   |     | NULL    |       |
| block_value_in        | decimal(30,0) | YES  |     | NULL    |       |
| block_value_out       | decimal(30,0) | YES  |     | NULL    |       |
| block_total_satoshis  | decimal(26,0) | YES  |     | NULL    |       |
| block_total_seconds   | decimal(20,0) | YES  |     | NULL    |       |
| block_satoshi_seconds | decimal(28,0) | YES  |     | NULL    |       |
| block_total_ss        | decimal(28,0) | YES  |     | NULL    |       |
| block_ss_destroyed    | decimal(28,0) | YES  |     | NULL    |       |
+-----------------------+---------------+------+-----+---------+-------+
"""
class ChainSumaryTable(Base):
    __tablename__ = 'chain_summary'
    chain_id = Column(DECIMAL(10,0),primary_key=True)
    in_longest = Column(DECIMAL(1,0))
    block_hash = Column(BINARY(32))
    block_version = Column(DECIMAL(10,0))
    block_hashMerkleRoot = Column(BINARY(32))
    block_nTime = Column(DECIMAL(20,0))
    block_nBits = Column(DECIMAL(10))
    block_nNonce = Column(DECIMAL(10))
    block_height = Column(DECIMAL(14,0))
    prev_block_id = Column(DECIMAL(14,0))
    prev_block_hash = Column(BINARY(32))
    block_chain_work = Column(BINARY(38))
    block_num_tx = Column(DECIMAL(10))
    block_value_in = Column(DECIMAL(30,0))
    block_value_out = Column(DECIMAL(30,0))
    block_total_satoshis = Column(DECIMAL(26,0))
    block_total_seconds = Column(DECIMAL(20,0))
    block_satoshi_seconds = Column(DECIMAL(28,0))
    block_total_ss = Column(DECIMAL(28,0))
    block_ss_destroyed = Column(DECIMAL(28,0))


"""
mysql> describe multisig_pubkey;
+-------------+---------------+------+-----+---------+-------+
| Field       | Type          | Null | Key | Default | Extra |
+-------------+---------------+------+-----+---------+-------+
| multisig_id | decimal(26,0) | NO   | PRI | NULL    |       |
| pubkey_id   | decimal(26,0) | NO   | PRI | NULL    |       |
+-------------+---------------+------+-----+---------+-------+
"""

class MultisigPubkeyTable(Base):
    __tablename__ = 'multisig_pubkey'
    multisig_id = Column(DECIMAL(26,0),primary_key=True)
    pubkey_id = Column(DECIMAL(26,0))


"""

mysql> describe orphan_block;
+----------------+---------------+------+-----+---------+-------+
| Field          | Type          | Null | Key | Default | Extra |
+----------------+---------------+------+-----+---------+-------+
| block_id       | decimal(14,0) | NO   | PRI | NULL    |       |
| block_hashPrev | binary(32)    | NO   | MUL | NULL    |       |
+----------------+---------------+------+-----+---------+---
"""

class OrphanBlockTable(Base):
    __tablename__ = 'orphan_block'
    block_id = Column(DECIMAL(14,0),primary_key=True)
    block_hashPrev = Column(BINARY(32))


"""
mysql> describe pubkey;
+-------------+---------------+------+-----+---------+-------+
| Field       | Type          | Null | Key | Default | Extra |
+-------------+---------------+------+-----+---------+-------+
| pubkey_id   | decimal(26,0) | NO   | PRI | NULL    |       |
| pubkey_hash | binary(20)    | NO   | UNI | NULL    |       |
| pubkey      | varbinary(65) | YES  |     | NULL    |       |
+-------------+---------------+------+-----+---------+-------+
"""

class PubkeyTable(Base):
    __tablename__ = 'pubkey'
    pubkey_id = Column(DECIMAL(26,0),primary_key=True)
    pubkey_hash = Column(BINARY(20))
    pubkey = Column(VARBINARY(65))



"""
mysql> describe pubkey_seq;
+-------+--------+------+-----+---------+----------------+
| Field | Type   | Null | Key | Default | Extra          |
+-------+--------+------+-----+---------+----------------+
| id    | bigint | NO   | PRI | NULL    | auto_increment |
+-------+--------+------+-----+---------+----------------+
"""

class PubkeySeqTable(Base):
    __tablename__ = 'pubkey_seq'
    id = Column(BIGINT(),primary_key=True)


"""
mysql> describe tx;
+-------------+---------------+------+-----+---------+-------+
| Field       | Type          | Null | Key | Default | Extra |
+-------------+---------------+------+-----+---------+-------+
| tx_id       | decimal(26,0) | NO   | PRI | NULL    |       |
| tx_hash     | binary(32)    | NO   | UNI | NULL    |       |
| tx_version  | decimal(10,0) | YES  |     | NULL    |       |
| tx_lockTime | decimal(10,0) | YES  |     | NULL    |       |
| tx_size     | decimal(10,0) | YES  |     | NULL    |       |
+-------------+---------------+------+-----+---------+-------+
"""

class TxTable(Base):
    __tablename__ = 'tx'
    tx_id = Column(DECIMAL(26,0),primary_key=True)
    tx_hash = Column(BINARY(32))
    tx_version = Column(DECIMAL(10,0))
    tx_lockTime = Column(DECIMAL(10,0))
    tx_size = Column(DECIMAL(10,0))


"""
mysql> describe tx_seq;
+-------+--------+------+-----+---------+----------------+
| Field | Type   | Null | Key | Default | Extra          |
+-------+--------+------+-----+---------+----------------+
| id    | bigint | NO   | PRI | NULL    | auto_increment |
+-------+--------+------+-----+---------+----------------+
"""
class TxSeqTable(Base):
    __tablename__ = 'tx_seq'
    id = Column(BIGINT(),primary_key=True)


"""
mysql> describe txin;
+----------------+---------------+------+-----+---------+-------+
| Field          | Type          | Null | Key | Default | Extra |
+----------------+---------------+------+-----+---------+-------+
| txin_id        | decimal(26,0) | NO   | PRI | NULL    |       |
| tx_id          | decimal(26,0) | NO   | MUL | NULL    |       |
| txin_pos       | decimal(10,0) | NO   |     | NULL    |       |
| txout_id       | decimal(26,0) | YES  | MUL | NULL    |       |
| txin_scriptSig | mediumblob    | YES  |     | NULL    |       |
| txin_sequence  | decimal(10,0) | YES  |     | NULL    |       |
+----------------+---------------+------+-----+---------+-------+
"""

class TxinTable(Base):
    __tablename__ = 'txin'
    txin_id = Column(DECIMAL(26,0),primary_key=True)
    tx_id = Column(DECIMAL(26,0))
    txin_pos = Column(DECIMAL(10,0))
    txout_id = Column(DECIMAL(26,0))
    txin_scriptSig = Column(MEDIUMBLOB())
    txin_sequence = Column(DECIMAL(10,0))

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


"""
mysql> describe txin_seq;
+-------+--------+------+-----+---------+----------------+
| Field | Type   | Null | Key | Default | Extra          |
+-------+--------+------+-----+---------+----------------+
| id    | bigint | NO   | PRI | NULL    | auto_increment |
+-------+--------+------+-----+---------+----------------+
"""
class TxinSeqTable(Base):
    __tablename__ = 'txin_seq'
    id = Column(BIGINT(), primary_key=True)


"""
mysql> describe txout;
+--------------------+---------------+------+-----+---------+-------+
| Field              | Type          | Null | Key | Default | Extra |
+--------------------+---------------+------+-----+---------+-------+
| txout_id           | decimal(26,0) | NO   | PRI | NULL    |       |
| tx_id              | decimal(26,0) | NO   | MUL | NULL    |       |
| txout_pos          | decimal(10,0) | NO   |     | NULL    |       |
| txout_value        | decimal(30,0) | NO   |     | NULL    |       |
| txout_scriptPubKey | mediumblob    | YES  |     | NULL    |       |
| pubkey_id          | decimal(26,0) | YES  | MUL | NULL    |       |
+--------------------+---------------+------+-----+---------+-------+
"""
class TxoutTable(Base):
    __tablename__ = 'txout'
    txout_id  = Column(DECIMAL(26,0), primary_key=True)
    tx_id = Column(DECIMAL(26,0))
    txout_pos = Column(DECIMAL(10,0))
    txout_value = Column(DECIMAL(30,0))
    txout_scriptPubKey = Column(MEDIUMBLOB())
    pubkey_id = Column(DECIMAL(26,0))


"""
mysql> describe txout_approx;
+--------------------+---------------+------+-----+---------+-------+
| Field              | Type          | Null | Key | Default | Extra |
+--------------------+---------------+------+-----+---------+-------+
| txout_id           | decimal(26,0) | NO   |     | NULL    |       |
| tx_id              | decimal(26,0) | NO   |     | NULL    |       |
| txout_approx_value | decimal(30,0) | NO   |     | NULL    |       |
+--------------------+---------------+------+-----+---------+-------+
"""
class TxoutApproxTable(Base):
    __tablename__ = 'txout_approx'
    txout_id  = Column(DECIMAL(26,0), primary_key=True)
    tx_id = Column(DECIMAL(26,0))
    txout_approx_value = Column(DECIMAL(30,0))



"""
mysql> describe txout_seq;
+-------+--------+------+-----+---------+----------------+
| Field | Type   | Null | Key | Default | Extra          |
+-------+--------+------+-----+---------+----------------+
| id    | bigint | NO   | PRI | NULL    | auto_increment |
+-------+--------+------+-----+---------+----------------+
mysql> 
"""
class TxoutSeqTable(Base):
    __tablename__ = 'txout_seq'
    id  = Column(BIGINT(), primary_key=True)






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






        self.block_fields = ['block_id','block_hash','block_version',
                             'block_hashMerkleRoot','block_nTime','block_nBits','block_nNonce',
                             'block_height','prev_block_id','search_block_id','block_chain_work',
                             'block_value_in','block_value_out','block_total_satoshis','block_total_seconds',
                             'block_satoshi_seconds','block_total_ss','block_num_tx','block_ss_destroyed'

                             ]


        self.block_next_fields = ['block_id','next_block_id']
        self.block_seq_fields = ['id']
        self.block_tx_fields = ['block_id','tx_id','tx_pos']
        self.block_txin_fields = ['block_id','txin_id','out_block_id']

        self.chain_fields = ['chain_id','chain_name','chain_code3','chain_address_version','chain_script_addr_vers',
                             'chain_magic','chain_policy','chain_decimals','chain_last_block_id',
                             'block_nNonce','block_height','prev_block_id','prev_block_hash',
                             'block_chain_work','block_num_tx','block_value_in','block_value_out',
                             'block_total_satoshis','block_total_seconds','block_satoshi_seconds','block_total_ss',
                             'block_ss_destroyed'

                             ]

        self.chain_candidate_fields = ['chain_id','block_id','in_longest','block_height']
        self.chain_seq_fields = ['id']


        self.chain_summary_fields = ['chain_id','in_longest','block_id','block_hash',
                                     'block_version','block_hashMerkleRoot','block_nTime','block_nBits',
                                     'block_nNonce','block_height','prev_block_id','prev_block_hash',
                                     'block_chain_work','block_num_tx','block_value_in','block_value_out',
                                     'block_total_satoshis','block_total_seconds','block_satoshi_seconds',
                                     'block_total_ss','block_ss_destroyed'

                                     ]

        self.multisig_pubkey_fields = ['multisig_id','pubkey_id']
        self.orphan_block_fields = ['block_id','block_hashPrev']
        self.pubkey_fields = ['pubkey_id','pubkey_hash','pubkey']
        self.pubkey_seq_fields = ['id']

        self.tx_fields = ['tx_id','tx_hash','tx_version','tx_lockTime',
                          'tx_size'
                          ]

        self.tx_seq_fields = ['id']

        self.txin_fields = ['txin_id','tx_id','txin_pos','txout_id',
                            'txin_scriptSig','txin_sequence'

                            ]

        self.txin_detail_fields = ['chain_id', 'in_longest', 'block_id', 'block_hash',
                                   'block_height', 'tx_pos', 'tx_id', 'tx_hash',
                                   'tx_lockTime', 'tx_version', 'tx_size', 'txin_id',
                                   'txin_pos', 'prevout_id', 'txin_scriptSig',
                                   'txin_sequence', 'txin_value', 'txin_scriptPubKey',
                                   'pubkey_id', 'pubkey_hash', 'pubkey'
                                   ]

        self.txin_seq_fields = ['id']

        self.txout_fields = ['txout_id','tx_id','txout_pos','txout_value',
                             'txout_scriptPubKey','pubkey_id'
                             ]


        self.txout_approx_fields = ['txout_id', 'tx_id', 'txout_approx_value']
        self.txout_seq_fields = ['id']





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
                        'chain_address_version','chain_script_addr_vers','chain_magic','prev_block_hash',
                        'pubkey_hash','pubkey','block_hashPrev','txin_scriptSig'


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
        Informational
        """
        print('\n',
              table,'\n',
              'The total number of rows is: '+str(self.__total_rows),'\n'
              'The chunk is:', self.__chunks, '\n',
              'The loop floor is:', self.__loop_floor_range, '\n',
              'The remainder is:', remainder)


        """
        Create .csv file and headers
        """

        for x in tq(range(self.__loop_floor_range)):

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
        self.set_up(table=BlockTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=BlockTable, fields=self.block_fields, file_name='block',
                        track_file='/block')






class BlockNextQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = {'block_id': 0, 'next_block_id': 0}


    def block_next_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=BlockNextTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=BlockNextTable, fields=self.block_next_fields, file_name='block_next',
                        track_file='/block_next')




class BlockSeqQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = {'id': 0}


    def block_seq_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=BlockSeqTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=BlockSeqTable, fields=self.block_seq_fields, file_name='block_seq',
                        track_file='/block_seq')



class BlockTxQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.block_tx_fields }


    def block_tx_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=BlockTxTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=BlockTxTable, fields=self.block_tx_fields, file_name='block_tx',
                        track_file='/block_tx')


class BlockTxinQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.block_txin_fields }


    def block_txin_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=BlockTxinTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=BlockTxinTable, fields=self.block_txin_fields, file_name='block_txin',
                        track_file='/block_txin')



class ChainQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.chain_fields}


    def chain_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=ChainTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=ChainTable, fields=self.chain_fields, file_name='chain',
                        track_file='/chain')




class ChainCandidateQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.chain_candidate_fields}


    def chain_candidate_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=ChainCandidateTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=ChainCandidateTable, fields=self.chain_candidate_fields, file_name='chain_candidate',
                        track_file='/chain_candidate')




class ChainSeqQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.chain_seq_fields}


    def chain_seq_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=ChainSeqTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=ChainSeqTable, fields=self.chain_seq_fields, file_name='chain_seq',
                        track_file='/chain_seq')



class ChainSummaryQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.chain_summary_fields}



    def chain_summary_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=ChainSumaryTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=ChainSumaryTable, fields=self.chain_summary_fields, file_name='chain_summary',
                        track_file='/chain_summary')


class MultisigPubkeyQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                         loop_floor_range, remainder, chunks,
                         clean_dict)

        self.clean_dict = {i: 0 for i in self.multisig_pubkey_fields}

    def multisig_pubkey_flow(self):
        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=MultisigPubkeyTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=MultisigPubkeyTable, fields=self.multisig_pubkey_fields, file_name='multisig_pubkey',
                        track_file='/multisig_pubkey')




class OrphanBlockQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.orphan_block_fields}


    def orphan_block_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=OrphanBlockTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=OrphanBlockTable, fields=self.orphan_block_fields, file_name='orphan_block',
                        track_file='/orphan_block')



class PubkeyQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.pubkey_fields}


    def pubkey_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=PubkeyTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=PubkeyTable, fields=self.pubkey_fields, file_name='pubkey',
                        track_file='/pubkey')



class PubkeySeqQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.pubkey_seq_fields}


    def pubkey_seq_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=PubkeySeqTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=PubkeySeqTable, fields=self.pubkey_seq_fields, file_name='pubkey_seq',
                        track_file='/pubkey_seq')




class TxQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.tx_fields}


    def tx_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=TxTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=TxTable, fields=self.tx_fields, file_name='tx',
                        track_file='/tx')



class TxSeqQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.tx_seq_fields}


    def tx_seq_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=TxSeqTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=TxSeqTable, fields=self.tx_seq_fields, file_name='tx_seq',
                        track_file='/tx_seq')




class TxinQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)


        self.clean_dict = { i : 0 for i in self.txin_fields}


    def txin_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=TxinTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=TxinTable, fields=self.txin_fields, file_name='txin',
                        track_file='/txin')





class TxinDetailQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)

        self.clean_dict = {i: 0 for i in self.txin_detail_fields}


    def txin_detail_flow(self):
        """
        Clean dict
        """

        self.set_clean_dict(self.clean_dict)


        """
        Setting up
        """
        self.set_up(table=TxinDetailTable, percentage=1)

        """
        Looping
        """
        self.loop_logic(table=TxinDetailTable, fields=self.txin_detail_fields, file_name='txin_detail',
                        track_file='/txin_detail')






class TxinSeqQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)

        self.clean_dict = {i: 0 for i in self.txin_seq_fields}


    def txin_seq_flow(self):
        """
        Clean dict
        """
        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=TxinSeqTable, percentage=1)

        """
        Looping
        """
        self.loop_logic(table=TxinSeqTable, fields=self.txin_seq_fields, file_name='txin_seq',
                        track_file='/txin_seq')




class TxoutQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)

        self.clean_dict = {i: 0 for i in self.txout_fields}


    def txout_flow(self):
        """
        Clean dict
        """
        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=TxoutTable, percentage=1)

        """
        Looping
        """
        self.loop_logic(table=TxoutTable, fields=self.txout_fields, file_name='txout',
                        track_file='/txout')




class TxoutApproxQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)

        self.clean_dict = {i: 0 for i in self.txout_approx_fields}


    def txout_approx_flow(self):
        """
        Clean dict
        """
        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=TxoutApproxTable, percentage=1)

        """
        Looping
        """
        self.loop_logic(table=TxoutApproxTable, fields=self.txout_approx_fields, file_name='txout_approx',
                        track_file='/txout_approx')





class TxoutDetailQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                 loop_floor_range, remainder, chunks,
                 clean_dict)

        self.clean_dict = {i: 0 for i in self.txout_detail_fields}


    def txout_detail_flow(self):
        """
        Clean dict
        """
        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=TxoutDetailTable, percentage=1)

        """
        Looping
        """
        self.loop_logic(table=TxoutDetailTable, fields=self.txout_detail_fields, file_name='txout_detail',
                        track_file='/txout_detail')



class TxoutSeqQuery(QueriesBase):
    def __init__(self, rows, percent, offset, floor_div,
                     loop_floor_range, remainder, chunks,
                     clean_dict):
        super().__init__(rows, percent, offset, floor_div,
                             loop_floor_range, remainder, chunks,
                             clean_dict)


        self.clean_dict = {i: 0 for i in self.txout_seq_fields}


    def txout_seq_flow(self):

        self.set_clean_dict(self.clean_dict)
        """
        Setting up
        """
        self.set_up(table=TxoutSeqTable, percentage=1)
        """
        Looping
        """
        self.loop_logic(table=TxoutSeqTable, fields=self.txout_seq_fields, file_name='txout_seq',
                        track_file='/txout_seq')












def main():
    """
    Creates .csv for txout_detail
    """

    """
    creates .csv for txin_detail
    """
    #a = QueriesBase(rows=0, percent=0, offset=0, floor_div=0,
                    #loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)

    b = TxinDetailQuery(rows=0, percent=0, offset=0, floor_div=0,
                        loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    b.txin_detail_flow()

    c = TxoutDetailQuery(rows=0, percent=0, offset=0, floor_div=0,
                  loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #c.txout_detail_flow()


    d = BlockQuery(rows=0, percent=0, offset=0, floor_div=0,
                   loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #d.block_flow()

    e = BlockNextQuery(rows=0, percent=0, offset=0, floor_div=0,
                   loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #e.block_next_flow()

    f = BlockSeqQuery(rows=0, percent=0, offset=0, floor_div=0,
                       loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #f.block_seq_flow()

    g = BlockTxQuery(rows=0, percent=0, offset=0, floor_div=0,
                      loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #g.block_tx_flow()

    h = BlockTxinQuery(rows=0, percent=0, offset=0, floor_div=0,
                     loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #h.block_txin_flow()

    h = ChainQuery(rows=0, percent=0, offset=0, floor_div=0,
                       loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #h.chain_flow()

    i = ChainCandidateQuery(rows=0, percent=0, offset=0, floor_div=0,
                   loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #i.chain_candidate_flow()

    j = ChainSeqQuery(rows=0, percent=0, offset=0, floor_div=0,
                            loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #j.chain_seq_flow()

    k = ChainSummaryQuery(rows=0, percent=0, offset=0, floor_div=0,
                      loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #k.chain_summary_flow()

    #l = MultisigPubkeyQuery(rows=0, percent=0, offset=0, floor_div=0,
                          #loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #l.multisig_pubkey_flow()

    m = OrphanBlockQuery(rows=0, percent=0, offset=0, floor_div=0,
                            loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #m.orphan_block_flow()

    n = PubkeyQuery(rows=0, percent=0, offset=0, floor_div=0,
                         loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #n.pubkey_flow()

    o = PubkeySeqQuery(rows=0, percent=0, offset=0, floor_div=0,
                    loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #o.pubkey_seq_flow()

    p = TxQuery(rows=0, percent=0, offset=0, floor_div=0,
                       loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #p.tx_flow()

    q = TxSeqQuery(rows=0, percent=0, offset=0, floor_div=0,
                loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #q.tx_seq_flow()

    r = TxinQuery(rows=0, percent=0, offset=0, floor_div=0,
                   loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #r.txin_flow()

    s = TxinSeqQuery(rows=0, percent=0, offset=0, floor_div=0,
                  loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #s.txin_seq_flow()

    t = TxoutQuery(rows=0, percent=0, offset=0, floor_div=0,
                     loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #t.txout_flow()

    u = TxoutApproxQuery(rows=0, percent=0, offset=0, floor_div=0,
                   loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #u.txout_approx_flow()

    v = TxoutSeqQuery(rows=0, percent=0, offset=0, floor_div=0,
                         loop_floor_range=0, remainder=0, chunks=0, clean_dict=None)
    #v.txout_seq_flow()


main()