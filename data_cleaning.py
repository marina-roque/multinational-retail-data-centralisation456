from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import numpy as np
import re
import pandas as pd


class DataCleaning:
    def __init__(self, data):
        self.original_data = data.copy()
        self.data = data
        
    def clean_user_data(self, legacy_users_table):
        try:
            pd.set_option('mode.chained_assignment', None)
            legacy_users_table.replace('NULL', np.NaN, inplace=True)
            legacy_users_table.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=True)

            legacy_users_table['date_of_birth'] = pd.to_datetime(legacy_users_table['date_of_birth'], errors = 'coerce')
            legacy_users_table['join_date'] = pd.to_datetime(legacy_users_table['join_date'], errors ='coerce')
            legacy_users_table = legacy_users_table.dropna(subset=['join_date'])

            legacy_users_table.loc[:, 'phone_number'] = legacy_users_table['phone_number'].str.replace('/W', '')
            legacy_users_table = legacy_users_table.drop_duplicates(subset=['email_address'])
            
            legacy_users_table['country_code'] = legacy_users_table['country_code'].replace('GGB', 'GB')
            country_code = ['GB', 'US', 'DE']
            mask = legacy_users_table['country_code'].isin(country_code)
            legacy_users_table = legacy_users_table[mask]
            legacy_users_table['country_code'] = legacy_users_table['country_code'].astype('category')
            
            allowed_countries = ['Germany', 'United Kingdom', 'United States']
            country_mask = legacy_users_table['country'].isin(allowed_countries)
            legacy_users_table = legacy_users_table[country_mask]
            legacy_users_table['country'] = legacy_users_table['country'].astype('category')
            
            legacy_users_table.drop(legacy_users_table.columns[0], axis=1, inplace=True)
            
            pd.set_option('mode.chained_assignment', 'warn')
            return legacy_users_table 
        except Exception as e:
            print(f"Error cleaning user data: {str(e)}")
            return None
        
    
    def get_original_data(self):
        return self.original_data.copy()
        
    def clean_card_data(self, data):
        try:
            pd.set_option('mode.chained_assignment', None)
            data = data.replace("NULL", np.NaN)
            data = data.drop_duplicates(subset=['card_number'])
            data = data.dropna()

            # Clean card number
            data['card_number'] = data['card_number'].apply(str)  # Converts it into a string
            data.loc[:, 'card_number'] = data['card_number'].str.replace('?', '')
            data = data[~data['card_number'].str.contains(r'[a-zA-Z]')]  # Drops rows with letters

            # Convert columns to datetime
            data["date_payment_confirmed"] = pd.to_datetime(data["date_payment_confirmed"], errors='coerce')
            # data["expiry_date"] = pd.to_datetime(data["expiry_date"], format='%m%Y', errors='coerce')

            print("Card cleaning done\n")
            
            pd.set_option('mode.chained_assignment', None)
            return data
        except Exception as e:
            print(f"Error cleaning card data: {str(e)}")
            return None
    
    def clean_store_data(self, store_data):
        try:
            # Reset index and replace 'NULL' with NaN
            store_data = store_data.reset_index(drop=True)
            store_data.replace('NULL', np.NaN, inplace=True)

            # Convert 'opening_date' to datetime
            store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], errors='coerce')

            # Individually replace values for specific rows in 'staff_numbers'
            store_data.loc[[31, 179, 248, 341, 375], 'staff_numbers'] = [78, 30, 80, 97, 39]

            # Convert 'staff_numbers' to numeric and drop rows with NaN
            store_data['staff_numbers'] = pd.to_numeric(store_data['staff_numbers'], errors='coerce')
            store_data.dropna(subset=['staff_numbers'], axis=0, inplace=True)

            # Replace values in 'continent'
            store_data['continent'] = store_data['continent'].str.replace('eeEurope', 'Europe').str.replace('eeAmerica', 'America')

            print("Store data cleaning done\n")
            return store_data
        except Exception as e:
            print(f"Error cleaning store data: {str(e)}")
            return None   
    
    def clean_products_data(self, products_df):
        if 'date_added' in products_df.columns:
            products_df['date_added'] = pd.to_datetime(products_df['date_added'], errors='coerce')
            products_df = products_df.dropna()
            products_df['product_price(£)'] = products_df['product_price'].replace('[£,]', '', regex=True).astype(float)
            products_df.drop('product_price', axis=1, inplace=True)
            products_df.rename(columns={'weight': 'weight(kg)'}, inplace=True)
            products_df['weight(kg)'] = products_df['weight(kg)'].round(2)
        return products_df    

    def split_weight_column(self, weight):
        if not weight['weight'].isna().all():  # Check if all values in the 'weight' column are NaN
            weight['weight'] = weight['weight'].astype(str)

            def split_element(element):
                match = re.match(r'(\d*\.?\d+)\s*([a-zA-Z]+)', str(element))
                if match:
                    numeric_value, unit = match.groups()
                    return pd.Series({'numeric_value': numeric_value, 'unit': unit})
                else:
                    return pd.Series({'numeric_value': None, 'unit': None})

            # Apply the split_element function to the 'weight' column and expand the result into separate columns
            weight[['numeric_value', 'unit']] = weight['weight'].apply(split_element)
            
            invalid_units = ['x', 'GO', None]
            weight = weight[~weight['unit'].isin(invalid_units)].copy()
        return weight
    
    def clean_weight_column(self, weight):
        weight = self.split_weight_column(weight)

        def process_element(row):
                numeric_value = row['numeric_value']
                unit = row['unit']

                # Convert numeric value to float
                numeric_value = float(numeric_value)

                # Convert to kg based on unit
                if unit.lower() == 'g':
                    return numeric_value / 1000
                elif unit.lower() == 'ml':
                    return numeric_value / 1000
                elif unit.lower() == 'oz':
                    return numeric_value * 0.0283495
                elif unit.lower() == 'kg':
                    return numeric_value
                else:
                    print(f"Unknown unit '{unit}' in weight. Skipping conversion.")
                    return row['weight']
        weight['weight'] = weight.apply(process_element, axis=1)
        weight.drop(['numeric_value', 'unit'], axis=1, inplace=True)
        return weight    
   
    def clean_orders_data(self, orders_table):
        try:
            columns_to_drop = ['first_name', 'last_name', '1', 'level_0', ]
            orders_table.drop(columns_to_drop, axis=1, inplace=True)
            return orders_table
        except Exception as e:
            print(f"Error cleaning orders data: {str(e)}")
                   
    def clean_dates_data(self, dates_table):
        try:
            dates_table.dropna(inplace=True)
            dates_table['year'] = dates_table['year'][dates_table['year'].str.isdigit()].astype('category')
            dates_table['month'] = dates_table['month'][dates_table['month'].str.isdigit()].astype('category')
            dates_table['day'] = dates_table['day'][dates_table['day'].str.isdigit()].astype('category')
            dates_table['time_period'] = dates_table.loc[dates_table['time_period'].str.isalpha(), 'time_period'].astype('category')
            dates_table.dropna()
            return dates_table
        except Exception as e:
            print(f"Error cleaning orders data: {str(e)}")
            
if __name__ == "__main__":
    # Create an instance of DatabaseConnector
    connector = DatabaseConnector('db_creds.yaml')
    
    data_extractor = DataExtractor(database_connector=connector)
    # List all tables in the database
    table_names = connector.list_rds_tables()
    #if table_names:
        #print("Tables in the database:", table_names)

    # List of CSV file paths for each table
    csv_file_paths = ['orders_table.csv', 'legacy_users.csv', 'legacy_store_details.csv']

    # Load CSV files into separate dataframes
    loaded_dataframes = data_extractor.load_csv_to_dataframe(csv_file_paths)
    legacy_users_df = loaded_dataframes['legacy_users']
    orders_table_df = loaded_dataframes['orders_table']
    #original_data = loaded_dataframes['legacy_users']
 
    # Create an instance of DataCleaning for legacy_users_df
    data_cleaning = DataCleaning(legacy_users_df)
    #cleaned_user_data = data_cleaning.clean_user_data(legacy_users_df)
    cleaned_user_data_table_name = 'dim_users'
    #connector.upload_to_local_db(data_cleaning.data, cleaned_user_data_table_name)


# Retrieve card details from the PDF
    card_details = data_extractor.retrieve_pdf_data()
    #clean_card_details = data_cleaning.clean_card_data(card_details)
    cleaned_card_data_table_name = 'dim_card_details'
    #connector.upload_to_local_db(clean_card_details, cleaned_card_data_table_name)
    cleaned_card_detail_df = data_extractor.retrieve_dim_card_details()
    
    
    api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    number_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    number_of_stores = data_extractor.list_number_of_stores()
    #print(f"Number of stores: {number_of_stores}")
    #store_data = data_extractor.retrieve_store_data(number_of_stores, retrieve_store_endpoint, api_key)
    #cleaned_store_data = data_cleaning.clean_store_data(store_data)
    #connector.upload_to_local_db(cleaned_store_data, 'dim_store_details', if_exists='append')
   
    s3_address = 's3://data-handling-public/products.csv'
    products_data_s3 = data_extractor.extract_from_s3()
    split_products_data = data_cleaning.split_weight_column(products_data_s3)
    converted_products_data = data_cleaning.clean_weight_column(split_products_data)
    #cleaned_products_data = data_cleaning.clean_products_data(converted_products_data)
    cleaned_products_data_table_name = 'dim_products'
    #connector.upload_to_local_db(cleaned_products_data, cleaned_products_data_table_name)
    
    orders_cleaning = DataCleaning(orders_table_df)
    #cleaned_orders_table = orders_cleaning.clean_orders_data(orders_table_df)
    cleaned_orders_table_name = 'orders_table'
    #connector.upload_to_local_db(cleaned_orders_table, cleaned_orders_table_name)
    
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_details_data = data_extractor.extract_from_s3_by_link()
    #cleaned_dates_table = data_cleaning.clean_dates_data(date_details_data)
    cleaned_dates_table_name = 'dim_dates_time'
    #connector.upload_to_local_db(cleaned_dates_table, cleaned_dates_table_name)