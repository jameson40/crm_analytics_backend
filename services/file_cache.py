import pandas as pd
import uuid
import os

# Кэш теперь внутри проекта
BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.abspath(os.path.join(BASE_DIR, "../cache"))
os.makedirs(CACHE_DIR, exist_ok=True)

def store_dataframe(df: pd.DataFrame | dict[str, pd.DataFrame]) -> str:
    file_id = str(uuid.uuid4())
    path = os.path.join(CACHE_DIR, f"{file_id}.pkl")

    if isinstance(df, pd.DataFrame) or isinstance(df, dict):
        pd.to_pickle(df, path)
    else:
        raise TypeError("store_dataframe ожидает DataFrame или dict[str, DataFrame]")

    return file_id

def get_dataframe(file_id: str, sheet: str | None = None) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
    path = os.path.join(CACHE_DIR, f"{file_id}.pkl")
    if not os.path.exists(path):
        return None

    data = pd.read_pickle(path)

    if sheet and isinstance(data, dict):
        return data.get(sheet)

    return data

def store_raw_excel(content: bytes) -> str:
    file_id = str(uuid.uuid4())
    path = os.path.join(CACHE_DIR, f"{file_id}.bin")
    with open(path, "wb") as f:
        f.write(content)
    return file_id

def get_raw_excel_bytes(file_id: str) -> bytes:
    path = os.path.join(CACHE_DIR, f"{file_id}.bin")
    if not os.path.exists(path):
        raise FileNotFoundError("Файл не найден")
    with open(path, "rb") as f:
        return f.read()
