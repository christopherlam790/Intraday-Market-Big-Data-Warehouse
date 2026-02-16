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
SILVER_ROOT = PROJECT_ROOT / "data" / "silver" / "cleaning"
SILVER_META = PROJECT_ROOT / "data" / "silver" / "cleaning_metadata"


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



# -----------------------
# Main func for cleaing parquets
# ----------------------


"""
Main func for cleaning - Makes cleaned silver parquet files
@param: str folder_path - path to bronze parquet files 
@returns: None
"""
def main(folder_path:str):    
    
    sorted_parquet_paths = list_files_alphabetically(folder_path=folder_path)

    ensure_dir(SILVER_ROOT)
    
    
    metadata = []
    
    
    
    # Itterate through each bronze parquet
    for parquet in sorted_parquet_paths:

        print(f"Analyzing parquet {parquet}")

        df = pd.read_parquet(parquet)
          
        # ------------------------
        # Verify necassary cols existence ['ID', 'TimeStamp', '/ES', '/NQ', '/RTY', 'SPY', 'QQQ', 'IWM'] 
        # ------------------------

        """
        Helper func for cleaning cols; Ensures necassary cols in df; Appends to metadata
        @param:pd.DataFrame df - df to clean
        @returns: pd.DataFrame - Col cleaned df
        """
        def clean_cols_for_existence(df: pd.DataFrame) -> pd.DataFrame:
        
     
            """
            Attempt access of valid cols; Fails if col does not exist

            @param:pd.DataFrame df - df to verify col exists in
            @returns: None
            """
            def verify_cols(df:pd.DataFrame) -> None:
                
                    
                for c in VALID_COLS:
                    temp = df[c]
                    
                return None
        
            # Verify Cols exist; Handle otherwise
            try:
                verify_cols(df=df)
                
                metadata.append(add_metadata.add_clean_metadata_instance(file=parquet,
                                                            layer= "silver",
                                                            process= "cleaning",
                                                            sub_process= "col_existence",
                                                            status= "conforming",
                                                            issue= "N/A",
                                                            action="processed",
                                                            notes="N/A"))
            except:
                
                #Handles schema issue 1 (See README.md)
                if len(df.columns) > 1:
                                                
                    # Step 1: Save current headers as first row
                    header_row = pd.DataFrame([df.columns], columns=df.columns)

                    # Step 2: Concatenate
                    df = pd.concat([header_row, df], ignore_index=True)

                    # Step 3: Let pandas auto-name columns (0, 1, 2, 3...)
                    df.columns = range(len(df.columns))

                    # Now you have numbered columns and old headers as first row
                
                    df.rename(columns={0: 'ID',
                                    1: 'TimeStamp',
                                    2: '/ES',
                                    3: '/NQ', 
                                    4: '/RTY', 
                                    5: 'SPY', 
                                    6: 'QQQ', 
                                    7: 'IWM'}, inplace=True)
                    
                    
                    metadata.append(add_metadata.add_clean_metadata_instance(file=parquet,
                                                            layer= "silver",
                                                            process= "cleaning",
                                                            sub_process= "col_existence",
                                                            status= "non_conforming",
                                                            issue= "silver_cleaning_col_1",
                                                            action="adjusted",
                                                            notes="Incorrect headers")) 
                                    
                    
                # Handles schema issue 2 (See README.md)
                else:
            
                    # Step 2: Get the problematic column name (the one with \t in it)
                    col_name = df.columns[0]

                    # Step 3: Split the column on tabs
                    df = df[col_name].str.split('\t', expand=True)

                    # Step 4: Split the header on tabs to get proper column names
                    proper_headers = col_name.split('\t')
                    df.columns = proper_headers

                    # Step 5: Save as properly structured parquet
                    df.to_parquet('fixed_file.parquet', index=False)
                    
                    metadata.append(add_metadata.add_clean_metadata_instance(file=parquet,
                                                            layer= "silver",
                                                            process= "cleaning",
                                                            sub_process= "col_existence",
                                                            status= "non_conforming",
                                                            issue= "silver_cleaning_col_2",
                                                            action="adjusted",
                                                            notes=".tsv"))
            
            
            # Reverify cols; Error if fails   
            try:
                verify_cols(df=df)
            except:
                raise Exception(f"An unknown error has occured on parquet {parquet}")
            
            
            
            return df
            
                
        df = clean_cols_for_existence(df)
        
        
        
        # ------------------
        # Verify necassary col's types
        # --------------------
        
        """
        Helper func for cleaning cols; Ensures necassary types in df; Appends to metadata
        @param:pd.DataFrame df - df to clean
        @returns: pd.DataFrame - Col cleaned df
        """ 
        def clean_cols_for_type(df: pd.DataFrame) -> pd.DataFrame:
        
            confoming_flag = True

            # Temporary convert all types to string to avoid parquet writing errs
            for col in df.columns:
                df[col] = df[col].astype(str)



            # Verify col types
            for col in VALID_COLS:
            
                col_type = df[col].dtype
            
                if col == "ID":
                    
                    try:
                        assert(col_type == "int64")
                        
                    except:
                        confoming_flag = False
                        
                        df[col] = df[col].astype("int64")
                        
                               
                elif col == "TimeStamp":
                    try:
                        assert(col_type == "object")
                    except:
                        confoming_flag = False
    
                        df[col] = df[col].astype("object")
    
                else:
                    try:
                        assert(col_type == "float64")
            
                    except:
                        confoming_flag = False
                        
                        df[col] = df[col].astype("float64")
                        

                # Reverify cols; Error if fails 
                
                col_type = df[col].dtype
            
                try:
                    if col == "ID":
                        assert(col_type == "int64")
    
                    elif col == "TimeStamp":
                        assert(col_type == "object")
                            
                    else:
                        assert(col_type == "float64")
                
                except:
                    raise Exception(f"An error has occured with col typing in parquet {parquet}")
                
                
            # Add metadata for status
            if confoming_flag:
                metadata.append(add_metadata.add_clean_metadata_instance(file=parquet,
                                                        layer= "silver",
                                                        process= "cleaning",
                                                        sub_process= "col_type",
                                                        status= "conforming",
                                                        issue= "N/A",
                                                        action="processed",
                                                        notes="N/A"))
            else:
                metadata.append(add_metadata.add_clean_metadata_instance(file=parquet,
                                                        layer= "silver",
                                                        process= "cleaning",
                                                        sub_process= "col_type",
                                                        status= "non_conforming",
                                                        issue= "silver_cleaning_col_3",
                                                        action="adjusted",
                                                        notes="Convert col types")) 
            
            return df
            
                            
        df = clean_cols_for_type(df=df)
        
        # --------------
        # Write prquets to folder
        # --------------
        dataset_name = Path(parquet).stem        

        dataset_name = dataset_name + "_cleaning"

        if not os.path.isfile(f"{SILVER_ROOT}/{dataset_name}.parquet"):
            print("Adding file")
        
            df.to_parquet(f"{SILVER_ROOT}/{dataset_name}.parquet", engine="pyarrow")
        
        
        
        

    # -----------------
    # Write metadata
    # -----------------

    ensure_dir(SILVER_META)

    with open(SILVER_META / "cleaning_metadata.json", "w") as json_file:
        json.dump(metadata, json_file, indent=4)
    
    json_file.close()
    
                
                
                
            
                
                


            
            

if __name__ == "__main__":
    
    main(folder_path="data/bronze/intraday_prices/")
    
    
    pass