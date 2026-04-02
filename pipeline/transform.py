from utils.logger import log
from typing import Any, Dict, List
import datetime as dt
import pandas as pd

logger = log(__name__)

class DateTimeHandlingErr(Exception):
    pass

# map values to standardize string or numerical values
def map_values(df: pd.DataFrame, col: str, val_map: Dict[Any, Any]) -> pd.DataFrame:

    try:

        if df.empty:
            raise ValueError("The DataFrame is Empty.")
        
        if col not in df.columns:
            raise ValueError(f"Column not found: {col}")

        df[col] = df[col].replace(val_map)
        return df
        
    except Exception as e:
        raise ValueError(f"The values cannot be mapped in column '{col}': {e}") from e

# standardizes datetime values in a col
def standardize_dt(df: pd.DataFrame, col: str, err: str, format: str) -> pd.DataFrame:

    VALID_ERRS = {"errors", "raise", "ignore"}

    if df.empty:
        raise ValueError("The DataFrame is Empty.")
    
    if err not in VALID_ERRS:
        raise ValueError(f"Invalid error parameter '{err}'. Must be one of: {VALID_ERRS}")
    
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}")


    logger.info(f"Standardizing column {col} into {format} format.")

    try:
        pass

        # base conversion here

        # null report

        # could be slicing specific parts or date normalization

    except Exception as e:
        # format inference
        pass
    except DateTimeHandlingErr as e:
        pass

# standardizes string values in a col
def standardize_str(df: pd.DataFrame, col: str, case_type: str) -> pd.DataFrame:
    
    logger.info(f"Converting column '{col}' into '{case_type}' case type.")

    df = df.copy()

    cases: dict = {
        "lower" : lambda s: s.astype(str).str.strip().str.lower(),
        "upper" : lambda s: s.astype(str).str.strip().str.upper(),
        "title" : lambda s: s.astype(str).str.strip().str.title()
    }

    try:

        if case_type not in cases:
            raise ValueError(f"Invalid Case Type: {case_type}")

        if col not in df.columns:
            raise KeyError(f"Column not found: {col}")
        
        logger.info(f"Transforming string cases from column '{col}'")

        transform = cases[case_type]
        df[col] = transform(df[col])

        return df

    except KeyError as e:
        raise ValueError(f"Column Error Occurred: {e}") from e
    
    except Exception as e:
        raise RuntimeError(f"Unexpected Transform Error: {e}") from e


# casts data types onto cols
def casting_dtypes(df: pd.DataFrame, col: str, dtype_map: Any) -> pd.DataFrame:

    logger.info(f"Casting Data Types for col '{col}'")

    try:
        df[col] = df[col].astype(dtype_map)
        return df
    except pd.errors.DtypeWarning as e:
        raise ValueError(f"Data Type Error: {e}")


# normalizes numerical columns
def normalize_col(df: pd.DataFrame, col: str) -> pd.Series:

    if col not in df.columns:
            raise KeyError(f"Column not found: {col}")
        
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise TypeError(f"Column '{col}' must be numeric.")
    
    try:
        
        minimum = df[col].min()
        maximum = df[col].max()

        if maximum == minimum:
            raise ValueError(f"Column '{col}' has no variance.")

        df[col] = (df[col] - minimum) / (maximum - minimum)

        return df
    
    except Exception as e:
        raise RuntimeError(f"Failed to normalize column '{col}': {e}") from e


# imputes nan values depending on impute func: mean, median, mode (categorical)
def impute_col(df: pd.DataFrame, col: str, impute_val: Any) -> pd.DataFrame:

    if df.empty:
        raise ValueError("The DataFrame is Empty.")
    
    if col not in df.columns:
        raise KeyError(f"Column not found: {col}")
        
    try:

        impute: dict = {
            "mean" : lambda val: val.fillna(val.mean()),
            "median" : lambda val: val.fillna(val.median()),
            "mode" : lambda val: val.fillna(val.mode()[0]) # can be also used for categorical data
        }

        if impute_val not in impute:
            raise ValueError(f"Invalid Impute Method. Must be one of: {set(impute.keys())}")

        df[col] = impute[impute_val](df[col])
        return df
    
    except Exception as e:
        raise RuntimeError(f"Failed to impute column {col}: {e}") from e

# aggregates columns
def aggregate_tbl(df: pd.DataFrame, columns: List[str], aggfunc: Dict[str, str]) -> pd.DataFrame:

    if df.empty:
        raise ValueError("The DataFrame is empty")

    missing_cols = [col for col in columns if col not in df.columns]  
    if missing_cols:
        raise KeyError(f"Column(s) not found: {missing_cols}")

    invalid_agg_cols = [col for col in aggfunc if col not in df.columns]
    if invalid_agg_cols:
        raise KeyError(f"aggfunc column(s) not found in DataFrame: {invalid_agg_cols}")

    try:
        aggregated_table = df.groupby(columns).agg(aggfunc).reset_index()
        return aggregated_table

    except Exception as e:
        raise RuntimeError(f"Failed to aggregate columns {columns}: {e}") from e

# merges tables from one another 
def merge_tbl(
        df: pd.DataFrame, tbl: str, left: str | None, right: str | None, how: str, on: str, suffixes: List[str]) -> pd.DataFrame:
    
    joins = {"inner", "outer", "left", "right"}


    if df.empty:
        raise ValueError("The DataFrame is empty")
    
    if left not in df.columns:
        raise KeyError(f"Left key '{left}' does not exist in the DataFrame.")
    
    if right not in tbl.columns:
        raise KeyError(f"Right key '{right}' does not exist in the DataFrame.")
    
    if how not in joins:
        raise ValueError(f"Invalid Join Parameter. Must be: {joins}")

    try:

        logger.info(f"Merging tables from table '{tbl}'")
        
        merged_tbl = df.merge(
            tbl,
            left_on=left,
            right_on=right,
            on=on,
            how=how,
            suffixes=suffixes
        )

        return merged_tbl

    except Exception as e:
        raise RuntimeError(f"Failed to merge rables: {e}") from e




