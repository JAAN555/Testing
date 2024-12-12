import os
import kagglehub

def ingest_data_from_kagglehub(dataset_id: str, destination_path: str = "./data") -> str:
    """
    Ingest a dataset from Kaggle using kagglehub.

    Args:
        dataset_id (str): The Kaggle dataset identifier (e.g., 'jacksoncrow/stock-market-dataset').
        destination_path (str): The local path where the dataset should be downloaded. Defaults to './data'.

    Returns:
        str: Path to the downloaded dataset files.
    """
    try:
        print(f"Attempting to download the dataset: {dataset_id}")
        download_path = kagglehub.dataset_download(dataset_id, destination=destination_path)
        print(f"Dataset successfully downloaded to: {download_path}")
        
        # Kontrollime, kas allalaaditud failid eksisteerivad
        if not os.listdir(destination_path):
            print(f"No files found in the destination path: {destination_path}")
            return None
        else:
            print(f"Files downloaded to: {destination_path}")
            return download_path
    except Exception as e:
        print(f"Failed to download the dataset: {dataset_id}")
        print(f"Error: {e}")
        return None
