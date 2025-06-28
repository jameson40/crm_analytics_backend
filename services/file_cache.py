import pandas as pd
import uuid
import os

# Кэш теперь внутри проекта
BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.abspath(os.path.join(BASE_DIR, "../cache"))
os.makedirs(CACHE_DIR, exist_ok=True)

def store_dataframe(df: pd.DataFrame) -> str:
    file_id = str(uuid.uuid4())
    path = os.path.join(CACHE_DIR, f"{file_id}.pkl")
    df.to_pickle(path)
    return file_id

def get_dataframe(file_id: str) -> pd.DataFrame | None:
    path = os.path.join(CACHE_DIR, f"{file_id}.pkl")
    if not os.path.exists(path):
        return None
    return pd.read_pickle(path)