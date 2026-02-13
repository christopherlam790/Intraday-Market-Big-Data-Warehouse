"""
Helper for adding metadata
"""



def add_clean_metadata_instance(file: str, layer: str, process:str, sub_process:str,
                                status: str, issue: str, action:str, notes: str):
    
    
    return {
        "file": file,
        "layer": layer,
        "process":process,
        "sub_process": sub_process,
        "status": status,
        "issue": issue,
        "action": action,
        "notes": notes,
    }
