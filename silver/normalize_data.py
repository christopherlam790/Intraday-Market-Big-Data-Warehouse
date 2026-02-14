


import pandas as pd
import os
from pathlib import Path
import pyarrow as pq
import json


import sys


root_dir = Path(__file__).resolve().parent.parent
helper_path = str(root_dir / "helpers")

if helper_path not in sys.path:
    sys.path.append(helper_path)


import add_metadata



PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_ROOT = PROJECT_ROOT / "data" / "silver" / "intraday_prices"
SILVER_META = PROJECT_ROOT / "data" / "silver" / "metadata"


VALID_COLS = ['ID', 'TimeStamp', '/ES', '/NQ', '/RTY', 'SPY', 'QQQ', 'IWM']

# ----------------------------
# Helpers
# ----------------------------

"""
Ensure directory of root is estavlished correctly

@param: Path path - Path of directory
@returns: None
"""
def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)



"""
Get list of files, ordered alphabeticaly for cleaning

@param:Path folder_path - path to folder
@param:str substring - optional substring that file path must abide by 
@returns: list[Path] - List of files, alphabetically
"""
def list_files_alphabetically(folder_path: Path, substring:str = "") -> list[Path]:
        
    paths = []
    
    # Access all parquet files
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        
        if substring in file_path:
        
            paths.append(file_path)
    
    return sorted(paths)


def main(folder_path:str):
        
    sorted_parquet_paths = list_files_alphabetically(folder_path=folder_path, substring="2025")

    ensure_dir(SILVER_ROOT)
    
    
    metadata = []
    
    
    
    # Itterate through each bronze parquet
    for parquet in sorted_parquet_paths:
        
        print(parquet)
        
        
if __name__ == "__main__":
    main(folder_path="data/silver/intraday_prices")