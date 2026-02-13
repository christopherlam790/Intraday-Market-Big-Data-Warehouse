"""
Write parquet files from Kaggle dataset for Bronze level
"""

from pathlib import Path
import kagglehub
import pandas as pd
import os
import json


import sys


root_dir = Path(__file__).resolve().parent.parent
helper_path = str(root_dir / "helpers")

if helper_path not in sys.path:
    sys.path.append(helper_path)


import add_metadata



from dotenv import load_dotenv

# ----------------------------
# Configuration
# ----------------------------

load_dotenv()


DATASET_ID = os.getenv("KAGGLE_PATH")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze" / "intraday_prices"
BRONZE_META = PROJECT_ROOT / "data" / "bronze" / "metadata" 

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

    metadata = []

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
                
                metadata.append(add_metadata.add_clean_metadata_instance(file=f"{BRONZE_ROOT}/{dataset_name}.parquet",
                                     layer="bronze",
                                    process= "ingest",
                                    sub_process= "N/A",
                                    status= "conforming",
                                    issue= "N/A",
                                    action= "processed",
                                    notes= "N/A"))
                                            
                
            else:
                print(f"Parquete {dataset_name} already exists")
                
        except:
            print(f"Unexpected error on dataset {dataset_name}")
            
            metadata.append(add_metadata.add_clean_metadata_instance(file=f"{BRONZE_ROOT}/{dataset_name}.parquet",
                                     layer="bronze",
                                    process= "ingest",
                                    sub_process= "N/A",
                                    status= "non_onforming",
                                    issue= "Unknown",
                                    action= "skipped",
                                    notes= "Skipped parquet due to unknown error"))
            
            continue
        
        
    
    print(f"Established {parquet_count} parquets")
    
    # -----------------
    # Write metadata
    # -----------------

    ensure_dir(BRONZE_META)

    with open(BRONZE_META / "ingestion_metadata.json", "w") as json_file:
        json.dump(metadata, json_file, indent=4)
    
    json_file.close()

        
        


if __name__ == "__main__":
    main()
