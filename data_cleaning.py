import pandas as pd
import re
import uuid

class DataCleaning:
    def clean_user_data(self, df):
        df = df.drop_duplicates()
        df["email_address"] = df["email_address"].apply(lambda x: x if re.match(r"[^@]+@[^@]+\.[^@]+", str(x)) else None)
        df = df[df["email_address"].notna()]
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")
        df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce")
        df = df[df["date_of_birth"].notna() & df["join_date"].notna()]
        df["user_uuid"] = df["user_uuid"].apply(lambda x: x if self.is_valid_uuid(str(x)) else None)
        df = df[df["user_uuid"].notna()]
        return df

    def is_valid_uuid(self, val):
        try:
            uuid.UUID(str(val))
            return True
        except:
            return False

    def clean_card_data(self, df):
        df = df.drop_duplicates()
        df["date_payment_confirmed"] = pd.to_datetime(df["date_payment_confirmed"], errors="coerce")
        df = df[df["date_payment_confirmed"].notna()]
        return df

    def clean_store_data(self, df):
        df = df.copy()
        df = df.dropna(subset=["store_code", "country_code", "continent", "address", "staff_numbers"])
        df["opening_date"] = pd.to_datetime(df["opening_date"], errors="coerce")
        df = df[df["opening_date"].notna()]
        df["staff_numbers"] = pd.to_numeric(df["staff_numbers"], errors="coerce")
        df = df[df["staff_numbers"].notna()]
        return df

    def convert_product_weights(self, df):
        def convert(value):
            try:
                value = str(value).lower().strip()
                if "x" in value:
                    parts = value.split("x")
                    if len(parts) == 2:
                        count = float(parts[0].strip())
                        unit_part = parts[1].strip()
                        if "kg" in unit_part:
                            return count * float(unit_part.replace("kg", "").strip())
                        elif "g" in unit_part:
                            return count * float(unit_part.replace("g", "").strip()) / 1000
                        elif "ml" in unit_part:
                            return count * float(unit_part.replace("ml", "").strip()) / 1000
                        elif "oz" in unit_part:
                            return count * float(unit_part.replace("oz", "").strip()) * 0.0283495
                elif "kg" in value:
                    return float(value.replace("kg", "").strip())
                elif "g" in value:
                    return float(value.replace("g", "").strip()) / 1000
                elif "ml" in value:
                    return float(value.replace("ml", "").strip()) / 1000
                elif "oz" in value:
                    return float(value.replace("oz", "").strip()) * 0.0283495
            except:
                return None
            return None

        df["weight"] = df["weight"].apply(convert)
        df = df[df["weight"].notna()]
        return df

    def clean_products_data(self, df):
        df = df.drop_duplicates()
        df = df[df["product_price"].notna()]
        df = df[df["weight"].notna()]
        return df
    
    def clean_orders_data(self, df):
        cols_to_drop = ["first_name", "last_name", "1", "level_0", "index"]
        for col in cols_to_drop:
            if col in df.columns:
                df = df.drop(columns=[col])
        return df
   
    def clean_date_times(self, df):
        df = df[df['date_uuid'].notna()]
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce').dt.time
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        df['day'] = pd.to_numeric(df['day'], errors='coerce')
        df = df.dropna(subset=['timestamp', 'year', 'month', 'day'])
        df['year'] = df['year'].astype(int)
        df['month'] = df['month'].astype(int)
        df['day'] = df['day'].astype(int)
        return df
