    from sqlalchemy import create_engine, text
    from utils.logger import log
    from typing import Any
    import requests
    import pandas as pd
    import os


    logger = log(__name__)

    class Pipeline:

    """
        Extracting Data
    """

    @staticmethod
    def fetch_file_data(path_name: str) -> pd.DataFrame:

        ext: str = os.path.splitext(path_name)[1].lower()

        """"
        filenames: str = {
            ".csv" : pd.read_csv(path_name),
            (".xls", '.xlsx') : pd.read_excel(path_name),
            ".json" : pd.read_json(path_name)
        }

        *planning to replace the if-else statements
        """

        try:
            
            if ext == ".csv":
                df = pd.read_csv(path_name)
            
            elif ext in [".xls", ".xlsx"]:
                df = pd.read_excel(path_name)
            
            elif ext == ".json":
                df = pd.read_json(path_name)
            
            else:
                raise Exception(f'Unsupported File Type')
            
            logger.info(f"Reading file {path_name}")
            
            return df
        
        except pd.errors.EmptyDataError as e:
            raise Exception(f'Empty Data: {e}')
        
        except FileNotFoundError:
            raise Exception(f'File path not found at {path_name}')
        
    @staticmethod
    def fetch_sql_query(query: str, db_engine: str) -> pd.DataFrame:

        engine = create_engine(db_engine)
        queries = text(query)

        logger.info("Executing SQL Query")

        try:    

            with engine.connect() as conn:
                data = pd.read_sql_query(queries, conn)
                if data.empty:
                    raise Exception("SQL Query showed no results")
                return data

        except pd.errors.DatabaseError as e:
            raise Exception(f"SQL Query Failed: {e}")
        
        except Exception as e:
            raise Exception(f'SQL Fetch Error: {e}')


    @staticmethod
    def fetch_json_data(url_path: str) -> pd.DataFrame:

        logger.info(f"Fetching API Data from: {url_path}")

        try:
            req = requests.get(url_path, timeout=10)
            req.raise_for_status()
            json_data = req.json()

            data = pd.json_normalize(json_data)

            if data.empty:
                raise Exception("Returned empty data.")
            return data

        except requests.exceptions.HTTPError as e:
            raise Exception(f'URL Fetch Error: {e}')        

        except requests.exceptions.Timeout as e:
            raise Exception(f"URL Request Timed Out: {e}")      
        
        except Exception as e:
            raise Exception(f"JSON Error {e}")

    """
        Transforming the Data
    """

    @staticmethod
    def map_values(df: pd.DataFrame, col: str, val_map: dict) -> pd.DataFrame:

        try:
            df[col] = df[col].replace(val_map)

            if df.empty:
                raise ValueError("The DataFrame is empty")
            return df
        except Exception as e:
            raise Exception(f"The values cannot be mapped")

    @staticmethod
    def standardize_str_cols(df: pd.DataFrame, col: str, case: str, val_map: dict | None = None) -> pd.DataFrame:
        
        df: pd.DataFrame = df.copy()

        """
        cases: str = {
        
            "upper" : lambda x, col: x[col].astype(str).str.strip().upper()
            ...

        }
        
        """





        try:

            if case == "upper":
                df[col] = df[col].astype(str).str.strip().upper()
            elif case == "lower":
                df[col] = df[col].astype(str).str.strip().lower()
            elif case == "title":
                df[col] = df[col].astype(str).str.strip().title()
            else:
                raise ValueError("The case must be: upper, lower, or title")
            
            if val_map:
                df = Pipeline.map_values(val_map)
                
            return df
        
        except Exception as e:
            raise ValueError(f"The DataFrame cannot be standardized.")

    @staticmethod
    def normalize_num_cols(df: pd.DataFrame, col: str, val_map: dict | None = None) -> pd.DataFrame:
        
        df: pd.DataFrame = df.copy()

        try:

            df[col] = pd.to_numeric(df[col], errors='coerce')

            if val_map:
                df = Pipeline.map_values(val_map)
        
        # still figuring out my logic on this one bru..

        except Exception as e:
            raise ValueError(f"")
        
        
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

    """
        ---Loading the Data---
    """

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