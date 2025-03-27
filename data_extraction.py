from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
import tabula
import requests
import time
import boto3
from io import StringIO


class DataExtractor:
    def list_tables(self, db_connector):
        return db_connector.list_db_tables()

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        return df

    def retrieve_pdf_data(self, link):
        dfs = tabula.read_pdf(link, pages='all', multiple_tables=True)
        return pd.concat(dfs)

    def list_number_of_stores(self, endpoint, header):
        response = requests.get(endpoint, headers=header)
        return response.json()["number_stores"]

    def retrieve_stores_data(self, store_endpoint, header, num_stores):
        data = []
        for i in range(num_stores):
            url = store_endpoint.format(store_number=i)
            for attempt in range(3):
                try:
                    response = requests.get(url, headers=header, timeout=10)
                    if response.status_code == 200:
                        data.append(response.json())
                        break
                except requests.exceptions.SSLError as e:
                    print(f"SSL error on store {i}, attempt {attempt+1}: {e}")
                except requests.exceptions.RequestException as e:
                    print(f"Request error on store {i}, attempt {attempt+1}: {e}")
                time.sleep(1.5)
        return pd.DataFrame(data)

    def extract_from_s3(self, s3_address):
        s3 = boto3.client("s3")
        bucket = s3_address.replace("s3://", "").split("/")[0]
        key = "/".join(s3_address.replace("s3://", "").split("/")[1:])
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(content))
        return df
    
    def extract_json_data(self, url):
        response = requests.get(url)
        response.raise_for_status()  
        return pd.read_json(StringIO(response.text))

if __name__ == "__main__":
    db = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    user_df = extractor.read_rds_table(db, "legacy_users")
    clean_users = cleaner.clean_user_data(user_df)
    print(f"Cleaned rows: {len(clean_users)}")
    print(clean_users.head())
    db.upload_to_db(clean_users, "dim_users")

    card_df = extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    print(f"Card rows: {len(card_df)}")
    print(card_df.head())
    clean_cards = cleaner.clean_card_data(card_df)
    print(f"Cleaned card rows: {len(clean_cards)}")
    print(clean_cards.head())
    db.upload_to_db(clean_cards, "dim_card_details")

    headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    num_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"

    store_count = extractor.list_number_of_stores(num_endpoint, headers)
    store_df = extractor.retrieve_stores_data(store_endpoint, headers, store_count)
    clean_stores = cleaner.clean_store_data(store_df)
    print(f"Cleaned store rows: {len(clean_stores)}")
    print(clean_stores.head())
    db.upload_to_db(clean_stores, "dim_store_details")



