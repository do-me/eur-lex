import os
import datetime
from huggingface_hub import HfApi

def upload():
    # 1. Get secrets from environment variables
    token = os.environ.get("HF_TOKEN")
    dataset_id = os.environ.get("HF_DATASET")
    
    if not token or not dataset_id:
        raise ValueError("HF_TOKEN or HF_DATASET environment variables are missing.")

    # Local directory where miner saved the parquet files. The miner writes
    # into year-subdirs (files/YYYY/dataset_*.parquet) that mirror the HF layout
    # 1:1, so we upload the contents of ./files/ into HF's files/ and the
    # directory structure is preserved without any path rewriting.
    local_folder_path = "./files"

    # 2. Initialize API
    print(f"Logging in and uploading files from {local_folder_path} to {dataset_id}...")
    api = HfApi(token=token)

    # 3. Upload Folder. Content-hash dedupe on HF means re-uploading unchanged
    # seeded files is a no-op; only new/modified parquets create commits.
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
