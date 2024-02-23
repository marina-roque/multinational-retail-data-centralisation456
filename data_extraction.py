from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import requests
import pandas as pd
from sqlalchemy import text
import csv
import tabula
import json
import boto3


'''This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.'''


class DataExtractor:
    def __init__(self, database_connector):
        self.db_connector = database_connector
        self.local_engine = database_connector.local_engine
            
    def extract_table_to_csv(self, table_name, csv_file_path):
        if not self.db_connector.db_engine:
            print("Database connection or engine not available. Exiting.")
            return

        try:
            conn = self.db_connector.db_engine.connect().execution_options(isolation_level='AUTOCOMMIT')

            # Execute SQL query for the current table
            query = text(f'SELECT * FROM {table_name}')
            result = conn.execute(query)

            # Fetch data and write to CSV file
            with open(csv_file_path, 'w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)

                # Write column headers
                csv_writer.writerow(result.keys())

                # Write data
                csv_writer.writerows(result.fetchall())

            print(f"Data saved to {csv_file_path}")
        except Exception as e:
            print(f"Error extracting data to CSV for table '{table_name}': {str(e)}")
        finally:
            conn.close()

    def load_csv_to_dataframe(self, csv_file_paths):
        try:
            dataframes = {}
            for csv_file_path in csv_file_paths:
                table_name = csv_file_path.split('.')[0]

                df = pd.read_csv(csv_file_path)
                dataframes[table_name] = df

            return dataframes
        except Exception as e:
            print(f"Error loading CSV files into DataFrames: {str(e)}")
            return None  
    
    def read_rds_table(self, table_name):
        try:
            query = f'SELECT * FROM {table_name}'
            return pd.read_sql_query(query, self.db_connector.rds_engine)
        except Exception as e:
            print(f"Error reading RDS table '{table_name}': {str(e)}")
            return pd.DataFrame()
        
    def retrieve_pdf_data(self):
        link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        card_details =  tabula.read_pdf(link, pages = "all") # Read pdf file
        card_details = pd.concat(card_details) # Put all multiple dataframe into a single dataframe
        return (card_details)
    

    def retrieve_dim_card_details(self):
        try:
            query = text('SELECT * FROM dim_card_details')
            with self.local_engine.connect() as conn:
                result = conn.execute(query)
            return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            print(f"Error retrieving data from dim_card_details: {str(e)}")
            return None 
        
    def API_key(self):
        return  {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

    def retrieve_store_data(self, number_stores, endpoint, api_key):
        try:
            data = []
            for store in range(0, number_stores):
                response = requests.get(f'{endpoint}{store}', headers=api_key)
                content = response.text
                result = json.loads(content)
                data.append(result)

            store_data = pd.DataFrame(data)
            return store_data
        except Exception as e:
            print(f"Error retrieving data store: {str(e)}")
            return None
        
                                
    def list_number_of_stores(self):
            api_url_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
            response = requests.get(
                                    api_url_base,
                                    headers=self.API_key()
                                    )
            return response.json()['number_stores']
        
    def extract_from_s3(self):
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket='data-handling-public', Key='products.csv')
        status   = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            return pd.read_csv(response.get("Body"))
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")

    def extract_from_s3_by_link(self):
        url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        response = requests.get(url) 
        dic      = response.json()
        products_data = pd.DataFrame([])
        for column_name in dic.keys():
            value_list = []
            for _ in dic[column_name].keys():
                value_list.append(dic[column_name][_])
            products_data[column_name] = value_list
        return products_data
        
        
    def extract_json_from_s3(self, s3_url):
        try:
            response = requests.get(s3_url)
            if response.status_code == 200:
                json_data = response.json()
                return json_data
            else:
                print("Failed to download JSON file from S3.")
                return None
        except Exception as e:
            print(f"Error extracting JSON from S3: {str(e)}")
            return None

