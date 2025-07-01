from io import BytesIO
from fastapi import APIRouter, UploadFile, Form
from typing import List, Optional
from pydantic import BaseModel
import pandas as pd

from services.excel_parser import load_excel_file, get_valid_excel_sheets, parse_excel_sheet_with_filters
from services.file_cache import store_dataframe, get_dataframe
from models.models import ExcelFilterRequest, AnalyzeExcelRequest

router = APIRouter()

@router.post("/list_excel_sheets")
def list_excel_sheets(file: UploadFile):
    content = file.file.read()
    buffer = BytesIO(content)

    excel_data = pd.read_excel(buffer, sheet_name=None)
    file_id = store_dataframe(excel_data)

    # нужно создать новый BytesIO, т.к. предыдущий уже прочитан
    buffer_for_listing = BytesIO(content)
    sheet_names = get_valid_excel_sheets(pd.ExcelFile(buffer_for_listing))

    return {
        "file_id": file_id,
        "sheets": sheet_names,
    }

@router.post("/get_excel_filters")
def get_excel_filters(req: ExcelFilterRequest):
    df = get_dataframe(req.file_id)
    sheet_df = pd.read_excel(df, sheet_name=req.sheet_name, nrows=100)

    filters = {}

    # Регион или Область
    if req.sheet_name in ["Действующие", "Завершенные", "Отказ и расторжение"]:
        region_col = "Регион"
    elif req.sheet_name in ["На модерации", "Отозванные"]:
        region_col = "Область"
    else:
        region_col = None

    if region_col and region_col in sheet_df.columns:
        filters["region"] = {
            "type": "select",
            "values": sorted(sheet_df[region_col].dropna().unique().tolist())
        }

    # Застройщик
    if "Застройщик" in sheet_df.columns:
        filters["developer"] = {
            "type": "select",
            "values": sorted(sheet_df["Застройщик"].dropna().unique().tolist())
        }

    # Площадь
    area_col = None
    if req.sheet_name in ["Действующие", "Завершенные"]:
        area_col = "Площадь, кв.м по Проекту"
    elif req.sheet_name in ["На модерации", "Отозванные"]:
        area_col = "Площадь"

    if area_col and area_col in sheet_df.columns:
        area_vals = pd.to_numeric(sheet_df[area_col], errors="coerce").dropna()
        filters["area"] = {
            "type": "range",
            "min": float(area_vals.min()),
            "max": float(area_vals.max())
        }

    # Период (только для Действующие и Завершённые)
    if req.sheet_name in ["Действующие", "Завершенные"]:
        date_start_col = "Дата начала строительства"
        date_end_col = "Дата завершения 2"  # можно расширить на "Дата по АПОЭ"

        if date_start_col in sheet_df.columns and date_end_col in sheet_df.columns:
            date_start = pd.to_datetime(sheet_df[date_start_col], errors="coerce")
            date_end = pd.to_datetime(sheet_df[date_end_col], errors="coerce")

            filters["period"] = {
                "type": "date_range",
                "min": str(date_start.min().date()),
                "max": str(date_end.max().date())
            }

    return filters

@router.post("/analyze_excel")
def analyze_excel(req: AnalyzeExcelRequest):
    df = get_dataframe(req.file_id)
    df_sheet = pd.read_excel(df, sheet_name=req.sheet_name)

    df_filtered = parse_excel_sheet_with_filters(df_sheet, req.sheet_name, req.filters)
    
    # Возвращаем базовую аналитику (заглушка)
    return {
        "rows_total": len(df_sheet),
        "rows_filtered": len(df_filtered),
        "summary": df_filtered.describe(include="all").to_dict()
    }
