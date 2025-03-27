import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self, path="db_creds.yaml"):
        with open(path, "r") as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine(
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
        
    def upload_to_db(self, df, table_name):
        engine = create_engine("postgresql://localhost:5432/sales_data")
        df.to_sql(table_name, engine, if_exists="replace", index=False)



if __name__ == "__main__":
    connector = DatabaseConnector()
    print("Tables:", connector.list_db_tables())

