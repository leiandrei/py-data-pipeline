from sqlalchemy import create_engine, text
from typing import Any
import requests
import pandas as pd
import os

class Pipeline:

    @staticmethod
    def fetch_file_data(path_name: str) -> pd.DataFrame:

        ext: str = os.path.splitext(path_name)[1].lower()

        try:
            
            if ext == ".csv":
                df = pd.read_csv(path_name)
            
            elif ext in [".xls", ".xlsx"]:
                df = pd.read_excel(path_name)
            
            elif ext == ".json":
                df = pd.read_json(path_name)
            
            else:
                raise Exception(f'Unsupported File Type')
            
            return df
            
        except FileNotFoundError:
            raise Exception(f'File path not found at {path_name}')
        
    @staticmethod
    def fetch_sql_query(query: str, db_engine: str) -> pd.DataFrame:

       engine = create_engine(db_engine)
       queries = text(query)

       try:
        with engine.connect() as conn:
            data = pd.read_sql_query(queries, conn)
            if data.empty:
                raise Exception("SQL Query showed no results")
            return data
        
       except Exception as e:
           raise Exception(f'SQL Fetch Error: {e}')

    
    @staticmethod
    def fetch_json_data(url_path: str) -> pd.DataFrame:

        url = url_path

        try:
            req = requests.get(url, timeout=10)
            req.raise_for_status()
            json_data = req.json()

            data = pd.json_normalize(json_data)

            if data.empty:
                raise Exception("Returned empty data.")
            return data

        except requests.exceptions.HTTPError as e:
            raise Exception(f'URL Fetch Error: {e}')               
        
        except Exception as e:
            raise Exception(f"JSON Error {e}")
        
    @staticmethod
    def data_aggregation(data: pd.DataFrame, group: list, agg_func: dict) -> pd.DataFrame:

        aggregated_tbl = data.groupby(group).agg(agg_func).reset_index()

        if aggregated_tbl.empty:
            raise Exception("Aggregated table is empty.")
        return aggregated_tbl

    @staticmethod  
    def impute_nan(df: pd.DataFrame, col: str, impute_value: Any) -> pd.DataFrame:

        try:

            df[col] = df[col].fillna(impute_value)

            if df[col].empty:
                raise ValueError("The dataframe series is empty")
            return df
        
        except KeyError:
            raise Exception(f"Column '{col}' not found.")
        
        except Exception as e:
            raise Exception(f"Unable to impute values: {e}")

    @staticmethod
    def load_data(data: pd.DataFrame, filename: str) -> None:

        ext: str = os.path.splitext(filename)[1].lower()

        try:
            if ext == ".csv":
                data.to_csv(filename, index=False)
            
            elif ext in [".xls", ".xlsx"]:
                data.to_excel(filename, index=False)
            
            elif ext == ".json":
                data.to_json(filename, orient='records')
        
        except Exception as e:
            raise Exception(f"Unsuppprted File Type: {e}")