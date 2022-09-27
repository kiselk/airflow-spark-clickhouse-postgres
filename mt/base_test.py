import pandas as pd
from sqlalchemy import create_engine
import os
import sys
import configparser
from with_exception import with_excepion


class BaseTest():
    def __init__(self, test_name):

        pd.set_option('display.expand_frame_repr', False)

        self.PWD_PATH = os.path.dirname(sys.argv[0])
        self.OUTPUT_FILE_NAME = test_name + '.csv'

        self.init_config()
        self.connect()

    def __del__(self):
        self.disconnect()

    @with_excepion
    def init_config(self):
        """Config initialization"""
        config = configparser.RawConfigParser()
        configFilePath = r'mt/config.ini'
        config.read(configFilePath)

        self.PG_SCHEMA = config.get('db', 'PG_SCHEMA')
        self.SQL_DIR_NAME = config.get('path', 'SQL_DIR_NAME')
        self.OUTPUT_DIR_NAME = config.get('path', 'OUTPUT_DIR_NAME')
        self.OUTPUT_PATH = os.path.join(self.PWD_PATH, self.OUTPUT_DIR_NAME,
                                        self.OUTPUT_FILE_NAME)

    @with_excepion
    def connect(self):
        """Connect"""

        self.alchemyEngine = create_engine(
            self.PG_SCHEMA, pool_recycle=3600)
        self.dbConnection = self.alchemyEngine.connect()

    @with_excepion
    def disconnect(self):
        """Disconnect"""
        self.dbConnection.close()

    @with_excepion
    def get_sql(self, procedure_name):
        """Getting sql contents by file _path"""

        file_path = os.path.join(
            self.PWD_PATH, self.SQL_DIR_NAME, procedure_name + '.sql')
        fd = open(file_path, 'r')
        sqlFile = fd.read()
        fd.close()
        return sqlFile
