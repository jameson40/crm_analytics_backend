import pandas as pd
import uuid
from typing import Dict

# Кэш для хранения загруженных таблиц
file_cache: Dict[str, pd.DataFrame] = {}

def store_dataframe(df: pd.DataFrame) -> str:
    file_id = str(uuid.uuid4())
    file_cache[file_id] = df
    return file_id

def get_dataframe(file_id: str) -> pd.DataFrame | None:
    return file_cache.get(file_id)