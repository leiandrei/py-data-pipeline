from sqlalchemy.exc import NoSuchTableError, OperationalError
from sqlalchemy import create_engine, text
from sqlalchemy import MetaData, Table
from bs4 import BeautifulSoup
from utils.logger import log

import pandas as pd
import requests
import os


logger = log(__name__)

# fetches from diff. file formats to a pandas df
def fetch_file(path_name: str, **kwargs) -> pd.DataFrame:
    
    ext: str = os.path.splitext(path_name)[1].lower()

    try:
    
        filenames: dict = {
            ".csv" : pd.read_csv,
            (".xls", ".xlsx") : pd.read_excel,
            ".json" : pd.read_json
        }

        logger.info(f"Reading the file from: {path_name}")

        for exts, reader, in filenames.items():
            
            if ext in exts:
                return reader(path_name)
            
        raise ValueError(f"Unsupported File Type: {ext}")
            
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

# fetches web data by scraping html tablesw
def web_data(url: str) -> pd.DataFrame:

    logger.info(f"Fetching Web Data from: {url}")

    try:

        r = requests.get(url)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")

        tables = pd.read_html(str(soup))

        if not tables:
            raise ValueError("No Tables Found on the Webpage.")
        
        return tables

    except requests.exceptions.HTTPError as e:
        raise ValueError(f"HTTP Error Occured: {e}") from e

# fetches API
def fetch_api(url: str, header: dict | None, params: dict | None) -> pd.DataFrame:

    logger.info(f"Fetch API Request from: {url}")

    try:
        response = requests.get(url, headers=header, params=params)
        response.raise_for_status()

        data = response.json
        return pd.json_normalize(data)

    except requests.exceptions.HTTPError as e:
        raise Exception(f"HTTP Error Occured: {e}") from e
    
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API Request Failed: {e}") from e

# checks overall data info (summary, null cols, duplicates)
def describe_data(df: pd.DataFrame) -> pd.DataFrame:
    pass

# checks db schema 
def check_schema(eng: str, table: str): 

    try:

        engine = create_engine(eng)
        metadata = MetaData()

        tbl = Table(table, metadata, autoload_with=engine)

        return tbl.columns.keys(), str(tbl)

    except NoSuchTableError as e:
        raise Exception(f"Table Not Found: {e}")
    
    except OperationalError as e:
        raise Exception(f"Database Connection Error: {e}")