from utils.logger import log
import pandas as pd

logger = log(__name__)

# still figuring out what to put in here
# same logic from the extract.py but it should return a clean data check

# planning to put test cases here
def data_quality(clean_df: pd.DataFrame) -> pd.Series:
    
    logger.info("Data Quality Check from the DataFrame")

    if clean_df.empty:
        raise ValueError("The DataFrame is Empty.")

    try:

        quality = pd.Series({
            "total_records" : len(clean_df),
            "duplicated_rows" : int(clean_df.duplicated().sum()),
            "missing_values" : int(clean_df.isna().sum().to_dict())
        })

        return quality

    except Exception as e:
        raise RuntimeError(f"Unexpected error during data inspection: {e}") from e

    except ValueError as e:
        raise RuntimeError(f"Invalid input: {e}") from e
