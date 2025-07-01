import os
import uuid
import pandas as pd
from typing import List
from io import BytesIO

from services.file_cache import CACHE_DIR

EXCLUDED_SHEETS = ["На рассмотрении"]

VALID_SHEETS = [
    "Действующие",
    "Завершенные",
    "На модерации",
    "Отозванные"
]

def load_excel_file(file_path: str) -> pd.ExcelFile:
    return pd.ExcelFile(file_path)

def get_valid_excel_sheets(excel: pd.ExcelFile) -> List[str]:
    return [s for s in excel.sheet_names if s in VALID_SHEETS and s not in EXCLUDED_SHEETS]

def parse_excel_sheet_with_filters(
    df: pd.DataFrame,
    sheet_name: str,
    filters: dict
) -> pd.DataFrame:
    df = df.copy()

    # Регион/Область
    region_col = None
    if sheet_name in ["Действующие", "Завершенные"]:
        region_col = "Регион"
    elif sheet_name in ["На модерации", "Отозванные"]:
        region_col = "Область"

    if region_col and filters.get("region"):
        df = df[df[region_col].isin(filters["region"])]

    # Застройщик
    if "developer" in filters:
        df = df[df["Застройщик"].isin(filters["developer"])]

    # Площадь
    area_col = None
    if sheet_name in ["Действующие", "Завершенные"]:
        area_col = "Площадь, кв.м по Проекту"
    elif sheet_name in ["На модерации", "Отозванные"]:
        area_col = "Площадь"

    if area_col and area_col in df.columns and "area" in filters:
        area_col_data = pd.to_numeric(df[area_col], errors="coerce")
        df = df[(area_col_data >= filters["area"]["min"]) & (area_col_data <= filters["area"]["max"])]

    # Период
    if sheet_name in ["Действующие", "Завершенные"] and "start_date" in filters and "end_date" in filters:
        date_start_col = "Дата начала строительства"
        date_end_col = "Дата завершения 2"

        df[date_start_col] = pd.to_datetime(df[date_start_col], errors="coerce")
        df[date_end_col] = pd.to_datetime(df[date_end_col], errors="coerce")

        start = pd.to_datetime(filters["start_date"])
        end = pd.to_datetime(filters["end_date"])

        df = df[(df[date_start_col] >= start) & (df[date_end_col] <= end)]

    return df

def read_excel_sheet(buffer: bytes, sheet_name: str) -> pd.DataFrame:
    header_map = {
        "Действующие": 6,
        "Завершенные": 2,
        "На модерации": 1,
        "Отозванные": 1
    }
    header_row = header_map.get(sheet_name, 0)

    excel = pd.ExcelFile(BytesIO(buffer))
    df = pd.read_excel(excel, sheet_name=sheet_name, header=header_row)
    return df