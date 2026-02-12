import pandas as pd
import os
from pathlib import Path
import pyarrow as pq


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_ROOT = PROJECT_ROOT / "data" / "silver" / "intraday_prices"

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
@returns: list[Path] - List of files, alphabetically
"""
def list_files_alphabetically(folder_path: Path) -> list[Path]:
        
    paths = []
    
    # Access all parquet files
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        
        paths.append(file_path)
    
    return sorted(paths)


def add_clean_metadata_instance(file: str, layer: str, process:str, status: str, issue: str, action:str, notes: str):
    
    
    return {
        "file": file,
        "layer": layer,
        "process":process,
        "status": status,
        "issue": issue,
        "action": action,
        "notes": notes,
    }


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
    
    
    cols = []
    
    
    # Itterate through each bronze parquet
    for parquet in sorted_parquet_paths:

        df = pd.read_parquet(parquet)
        
        
        
        # ------------------------
        # Verify necassary cols existence ['ID', 'TimeStamp', '/ES', '/NQ', '/RTY', 'SPY', 'QQQ', 'IWM'] 
        # ------------------------

        """
        Helper func for cleaning cols; Ensures necassary cols in df
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
                
                metadata.append(add_clean_metadata_instance(file=parquet,
                                                            layer= "silver",
                                                            process= "cleaning",
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
                    
                    
                    metadata.append(add_clean_metadata_instance(file=parquet,
                                                            layer= "silver",
                                                            process= "cleaning",
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
                    
                    metadata.append(add_clean_metadata_instance(file=parquet,
                                                            layer= "silver",
                                                            process= "cleaning",
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
        
        
        def clean_cols_for_type(df: pd.DataFrame) -> pd.DataFrame:
            
            
            try:
                for col in VALID_COLS:
                
                    col_type = df[col].dtype
                
                    if col == "ID":

                        assert(col_type == "int64")

                        
                    elif col == "TimeStamp":
                        assert(col_type == "object")

                        
                    else:
                        assert(col_type == "float64")
                
            except:
                print(f"Error in parquet {parquet}")
               
                
        
        
        clean_cols_for_type(df=df)
        
        
        # Helper for verifying curr cols
        for c in list(df.columns):
            if c not in cols:
                #print(f"Adding col {c} from parquet {parquet}")
                cols.append(c)


    print(cols)
    
    print(metadata)
                
                
                
            
                
                


            
            

if __name__ == "__main__":
    
    main(folder_path="data/bronze/intraday_prices/")
    
    
    pass