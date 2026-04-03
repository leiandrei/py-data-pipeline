from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
from typing import Dict, Callable
from utils.logger import log
import pandas as pd

logger = log(__name__)

# function to load a data file to a specific file ext and path
def to_file(clean_data: pd.DataFrame, path_name: str, ext: str) -> None:

    paths: Dict[str, Callable] = {
        "csv" : lambda: clean_data.to_csv(path_name, index=False),
        "xls" : lambda: clean_data.to_excel(path_name, index=False),
        "xlsx" : lambda: clean_data.to_excel(path_name, index=False),
        "json" : lambda: clean_data.to_json(path_name, orient='records')
    }
    
    if clean_data.empty:
        raise ValueError("The DataFrame is Empty")  
    
    logger.info(f"Loading Clean Data into '{ext}' file extension")

    if ext not in paths:
        raise ValueError(f"Unsupported File Type: {ext} . Supported Files: {set(paths.keys())}")

    try:
        paths[ext]()
        logger.info(f"Data saved successfully to: '{path_name}'")
    
    except Exception as e:
        raise RuntimeError(f"Failed to save file to path: '{path_name}': {e}") from e
    
def to_sql(clean_data: pd.DataFrame, tbl_name: str, eng: str) -> None:

    if clean_data.empty:
        raise ValueError("The DataFrame is Empty")  

    logger.info(f"Loading Clean Data into Table '{tbl_name}'")
    logger.info(f"Database Engine: '{eng}'")

    engine = create_engine(eng)

    try:
        clean_data.to_sql(tbl_name, engine, if_exists='replace', index=False)

        logger.info(f"Successfully loaded {len(clean_data)} rows into '{tbl_name}'")

    except SQLAlchemyError as e:
        raise RuntimeError(f"Loading Clean Data to a Database Failed: {e}") from e
    
    finally:
        engine.dispose()
        

        

        