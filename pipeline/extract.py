from sqlalchemy.exc import NoSuchTableError, OperationalError
from sqlalchemy import create_engine, text
from sqlalchemy import MetaData, Table
from bs4 import BeautifulSoup
from typing import Dict, Tuple, Callable
from utils.logger import log

import pandas as pd
import requests
import os

# exception class
class QualityErr(Exception):
    pass

logger = log(__name__)

# fetches from diff. file formats to a pandas df
def fetch_file(path_name: str) -> pd.DataFrame:
    
    ext: str = os.path.splitext(path_name)[1].lower()

    if not os.path.exists(path_name):
        raise FileNotFoundError(f"File not found: {path_name}")

    try:
    
        filenames: Dict[str, Callable] = {
            ".csv" : pd.read_csv,
            ".xls" : pd.read_excel,
            ".xlsx" : pd.read_excel,
            ".json" : pd.read_json
        }

        logger.info(f"Reading the file from: {path_name}")

        reader = filenames.get(ext)

        if reader is None:
            raise ValueError(f"Unsupported File Type: {ext}")
        return reader(path_name)
            
    except pd.errors.EmptyDataError as e:
        raise ValueError(f"The File is Empty: {e}") from e
    
    except pd.errors.ParserError as e:
        raise ValueError(f"File Parsing Error Occurred: {e}") from e


# fetches db engine and queries to return a 
def query_db(eng: str, queries: str) -> pd.DataFrame:

    engine = create_engine(eng)
    query = text(queries)

    logger.info(f"Fetching from a SQL Database.")

    try:

        with engine.connect() as conn:
            data: pd.DataFrame = pd.read_sql_query(query, conn)
            if data.empty:
                raise ValueError("The following SQL query is empty.")
            return data

    except Exception as e:
        raise Exception(f"Database Fetch Error: {e}") from e
    finally:
        engine.dispose()

# fetches web data by scraping html tablesw
def web_data(url: str) -> pd.DataFrame:

    logger.info(f"Fetching Web Data from: {url}")

    try:

        r = requests.get(url, timeout=10)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")

        tables = pd.read_html(str(soup))

        if not tables:
            raise ValueError("No Tables Found on the Webpage.")
        
        return tables[0]

    except requests.exceptions.HTTPError as e:
        raise ValueError(f"HTTP Error Occured: {e}") from e

# fetches API
def fetch_api(url: str, header: dict | None, params: dict | None) -> pd.DataFrame:

    logger.info(f"Fetch API Request from: {url}")

    try:
        response = requests.get(url, headers=header, params=params)
        response.raise_for_status()

        logger.info(f"API Fetch Status: {response.status_code}")

        data = response.json()
        return pd.json_normalize(data)

    except requests.exceptions.HTTPError as e:
        raise Exception(f"HTTP Error Occured: {e}") from e
    
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API Request Failed: {e}") from e

# checks overall data info (summary, null cols, duplicates)
def data_quality(df: pd.DataFrame) -> pd.Series:
    
    logger.info("Data Quality Check from the DataFrame")

    if df.empty:
        raise ValueError("The DataFrame is Empty.")

    try:

        quality = pd.Series({
            "total_records" : len(df),
            "duplicated_rows" : int(df.duplicated().sum()),
            "missing_values" : df.isna().sum().to_dict()
        })

        return quality

    except ValueError as e:
        raise RuntimeError(f"Invalid input: {e}") from e

    except QualityErr as e:
        raise RuntimeError(f"The DataFrame cannot be inspected: {e}") from e
    
    except Exception as e:
        raise RuntimeError(f"Unexpected error during data inspection: {e}") from e


# checks db schema 
def check_schema(eng: str, table: str) -> Tuple:

    try:

        engine = create_engine(eng)
        metadata = MetaData()

        tbl = Table(table, metadata, autoload_with=engine)

        return tbl.columns.keys(), str(tbl)

    except NoSuchTableError as e:
        raise Exception(f"Table Not Found: {e}")
    
    except OperationalError as e:
        raise Exception(f"Database Connection Error: {e}")
    
    finally:

        engine.dispose()