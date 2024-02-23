from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect
'''I will use this to connect with and upload data to the database.'''


class DatabaseConnector:
    '''I will use this to connect with and upload data to the database.'''
    def __init__(self, file_path):
        self.credentials = self.load_credentials(file_path)
        self.rds_engine = self.init_db_engine(self.credentials, prefix='RDS_')
        self.local_engine = self.init_db_engine(self.load_local_credentials(), prefix='LOCAL_')
            
    def load_local_credentials(self):
        try:
            with open('db_creds.yaml', 'r') as file:
                credentials = yaml.safe_load(file)
            return credentials
        except Exception as e:
            print(f"Error loading local credentials: {str(e)}")
            return None
        
    def load_credentials(self, file_path):
        try:
            with open(file_path, 'r') as file:
                credentials = yaml.safe_load(file)
            return credentials
        except Exception as e:
            print(f"Error loading credentials from {file_path}: {str(e)}")
            return None
    
    def init_db_engine(self, credentials, prefix=''):
        try:
            engine = create_engine(
                f"postgresql://{credentials[prefix + 'USER']}:{credentials[prefix + 'PASSWORD']}@{credentials[prefix + 'HOST']}:{credentials[prefix + 'PORT']}/{credentials[prefix + 'DATABASE']}"
            )
            engine.execution_options(isolation_level='AUTOCOMMIT')
            return engine
        except Exception as e:
            print(f"Error creating database engine: {str(e)}")
            return None
        
    def list_rds_tables(self):
        if not self.rds_engine:
            print("RDS engine not available. Exiting.")
            return []

        inspector = inspect(self.rds_engine)
        table_names = inspector.get_table_names()
        return table_names
                    
    def upload_to_local_db(self, df, table_name, if_exists='replace'):
        try:
            df.to_sql(table_name, self.local_engine, if_exists = if_exists)
            print(f"Data uploaded to table '{table_name}' in the local database successfully.")
        except Exception as e:
            print(f"Error uploading data to local table '{table_name}': {str(e)}")
        
        

    