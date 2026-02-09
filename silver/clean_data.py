import pandas as pd
import os


def clean_all_parquet_files(folder_path:str):
    
    
    # Access all parquete files
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)



        succ_count = 0
        error_count = 0

        if os.path.isfile(file_path):
            try:
                df = pd.read_parquet(file_path)
                
                print(file_path)
                
                
                
                
                succ_count += 1
            except Exception as e:
                error_count += 1
                print(f"Error reading file {filename}: {e}")
                
    print(error_count)



if __name__ == "__main__":
    
    clean_all_parquet_files(folder_path="data/bronze/intraday_prices/")
    
    
    pass