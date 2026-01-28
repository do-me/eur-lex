import os
import datetime
from huggingface_hub import HfApi

def upload():
    # 1. Get secrets from environment variables
    token = os.environ.get("HF_TOKEN")
    dataset_id = os.environ.get("HF_DATASET")
    
    if not token or not dataset_id:
        raise ValueError("HF_TOKEN or HF_DATASET environment variables are missing.")

    # Local directory where miner saved the parquet files
    local_folder_path = "./files"
    
    # 2. Initialize API
    print(f"Logging in and uploading files from {local_folder_path} to {dataset_id}...")
    api = HfApi(token=token)
    
    # 3. Upload Folder
    # path_in_repo="files" creates a 'files' directory in the HF repo
    api.upload_folder(
        folder_path=local_folder_path,
        repo_id=dataset_id,
        repo_type="dataset",
        path_in_repo="files", 
        ignore_patterns=[".gitkeep", ".DS_Store"],
        commit_message=f"Weekly update: {datetime.date.today()}"
    )
    print("Upload complete!")

if __name__ == "__main__":
    upload()