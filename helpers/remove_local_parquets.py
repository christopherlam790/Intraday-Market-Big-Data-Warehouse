"""
Helper for deleting local parquete files
"""


import pandas as pd
from pathlib import Path


    
import shutil
import os

def main(folder_path: str) -> None:
    
    if not input(f"Are you sure you want to eliminate all data in folder path {folder_path}? (Y/N)") == "Y":
        print("No files have been deleted")
        return 


    # Check if the folder exists before attempting to delete to prevent FileNotFoundError
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        try:
            shutil.rmtree(folder_path)
            print(f"Folder and all contents at '{folder_path}' deleted successfully.")
        except OSError as e:
            print(f"Error: {folder_path} : {e.strerror}")
    else:
        print(f"Folder not found or is not a directory at '{folder_path}'.")





if __name__ == "__main__":
    main("data/silver/intraday_prices")
