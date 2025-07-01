from io import BytesIO
from fastapi import APIRouter, UploadFile
from typing import List
import pandas as pd

from services.excel_parser import (
    get_valid_excel_sheets,
    parse_excel_sheet_with_filters,
    read_excel_sheet
)
from services.file_cache import (
    store_raw_excel,
    get_raw_excel_bytes
)
from models.models import ExcelFilterRequest, AnalyzeExcelRequest

router = APIRouter()

@router.post("/list_excel_sheets")
def list_excel_sheets(file: UploadFile):
    content = file.file.read()
    file_id = store_raw_excel(content)

    buffer = BytesIO(content)
    excel = pd.ExcelFile(buffer)
    sheet_names = get_valid_excel_sheets(excel)

    return {
        "file_id": file_id,
        "sheets": sheet_names,
    }

@router.post("/get_excel_filters")
def get_excel_filters(req: ExcelFilterRequest):
    buffer = get_raw_excel_bytes(req.file_id)
    sheet_df = read_excel_sheet(buffer, req.sheet_name)

    if sheet_df.empty:
        return {"error": "Пустой лист"}

    filters = {}

    # Регион или Область
    if req.sheet_name in ["Действующие", "Завершенные"]:
        region_col = "Регион"
    elif req.sheet_name in ["На модерации", "Отозванные"]:
        region_col = "Область"
    else:
        region_col = None

    if region_col and region_col in sheet_df.columns:
        raw_values = sheet_df[region_col].dropna().unique().tolist()
        filters["region"] = {
            "type": "select",
            "values": sorted([str(v) for v in raw_values])
        }

    # Застройщик
    if "Застройщик" in sheet_df.columns:
        devs = sheet_df["Застройщик"].dropna().unique().tolist()
        filters["developer"] = {
            "type": "select",
            "values": sorted(devs)
        }

    # Площадь
    area_col = None
    if req.sheet_name in ["Действующие", "Завершенные"]:
        area_col = "Площадь, кв.м по Проекту"
    elif req.sheet_name in ["На модерации", "Отозванные"]:
        area_col = "Площадь"

    if area_col and area_col in sheet_df.columns:
        area_vals = pd.to_numeric(sheet_df[area_col], errors="coerce").dropna()
        if not area_vals.empty:
            filters["area"] = {
                "type": "range",
                "min": float(area_vals.min()),
                "max": float(area_vals.max())
            }

    # Период (только для Действующие и Завершённые)
    if req.sheet_name in ["Действующие", "Завершенные"]:
        date_start_col = "Дата начала строительства"
        date_end_col = "Дата завершения 2"

        if date_start_col in sheet_df.columns and date_end_col in sheet_df.columns:
            date_start = pd.to_datetime(sheet_df[date_start_col], errors="coerce")
            date_end = pd.to_datetime(sheet_df[date_end_col], errors="coerce")

            valid_start = date_start.dropna()
            valid_end = date_end.dropna()

            if not valid_start.empty and not valid_end.empty:
                filters["period"] = {
                    "type": "date_range",
                    "min": str(valid_start.min().date()),
                    "max": str(valid_end.max().date())
                }

    return filters

@router.post("/analyze_excel")
def analyze_excel(req: AnalyzeExcelRequest):
    buffer = get_raw_excel_bytes(req.file_id)
    df_sheet = read_excel_sheet(buffer, req.sheet_name)

    df_filtered = parse_excel_sheet_with_filters(df_sheet, req.sheet_name, req.filters)

    return {
        "rows_total": len(df_sheet),
        "rows_filtered": len(df_filtered),
        "summary": df_filtered.describe(include="all").to_dict()
    }
