import pandas as pd
import os

# just imported this from the old pipeline class, gon' refactor this later on

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