"""
Write parquet files from Kaggle dataset for Bronze level
"""

from pathlib import Path
import kagglehub
import pandas as pd
import os


from dotenv import load_dotenv

# ----------------------------
# Configuration
# ----------------------------

load_dotenv()


DATASET_ID = os.getenv("KAGGLE_PATH")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze" / "intraday_prices"


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
Get list of files that are of format csv

@param:Path dataset_path - path to dataset
@returns: list[Path] - List of *.csv paths
"""
def list_csv_files(dataset_path: Path) -> list[Path]:
    return list(dataset_path.rglob("*.csv"))


# ----------------------------
# Main Ingestion Logic
# ----------------------------

"""
Main func for ingestion - Makes parquety files
@param:bool overwrite - bool indicating if to overwrite if file already exists; False by default 
@returns: None
"""
def main(overwrite: bool = False) -> None:
    print("Downloading dataset from Kaggle...")
    dataset_path = Path(kagglehub.dataset_download(DATASET_ID))

    csv_files = list_csv_files(dataset_path)
    if not csv_files:
        raise FileNotFoundError("No CSV files found in Kaggle dataset")

    ensure_dir(BRONZE_ROOT)

    parquet_count = 0

    for path in csv_files:
        
        try:
              
            dataset_name = Path(path).stem

            
            # Fix single entry error
            if dataset_name == "TOS Kaggle data week ending 2024 09 013csv":
                dataset_name = "TOS Kaggle data week ending 2024 09 13"


            skip_flag = False
            # Ignore the example and any file not organized properly
            if "Zipped data" not in str(path) or dataset_name == "TOS Kaggle data example.parquet":            
                skip_flag = True
            
            
            # Only establish if not a skip file, overwrite is false, and does not already exist
            if not skip_flag and (overwrite or not os.path.exists(f"{BRONZE_ROOT}/{dataset_name}.parquet")):
                df = pd.read_csv(path)
                
                df.to_parquet(f"{BRONZE_ROOT}/{dataset_name}.parquet", engine="pyarrow")
            
                print(df)
            
                parquet_count += 1
            else:
                print(f"Parquete {dataset_name} already exists")
                
        except:
            print(f"Unexpected error on dataset {dataset_name}")
            continue
        
    print(f"Established {parquet_count} parquets")

        
        


if __name__ == "__main__":
    main()
