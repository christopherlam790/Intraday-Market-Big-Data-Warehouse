"""
Helper for reading parquete files
"""


import pandas as pd
from pathlib import Path



def main(parquete_path: str) -> None:
    
    try:
        df = pd.read_parquet(parquete_path)
        
        print(df)
    except FileNotFoundError:
        print(f"File DNE on path: {parquete_path}")




if __name__ == "__main__":
    main("fixed_file.parquet")
    main("data/silver/intraday_prices/TOS Kaggle data week ending 2025 08 08_cleaning.parquet")