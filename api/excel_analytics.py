from fastapi import APIRouter, UploadFile, File, Form, Query
from fastapi.responses import JSONResponse
import pandas as pd
import traceback
import json

from services.excel_parser import parse_excel, clean_excel_dataframe
from usecases.excel_analyze_deals import apply_filters, compute_summary
from services.file_cache import store_dataframe, get_dataframe

router = APIRouter()

@router.post("/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    try:
        sheet_dfs = parse_excel(file.file)
        cleaned_sheets = {}

        for sheet_name, df in sheet_dfs.items():
            df_cleaned = clean_excel_dataframe(df)
            cleaned_sheets[sheet_name] = df_cleaned  # сохраняем по листам

        file_id = store_dataframe(cleaned_sheets)
        print(f"[UPLOAD EXCEL] Загружено листов: {len(cleaned_sheets)}, file_id: {file_id}")
        return {"status": "ok", "file_id": file_id}
    except Exception as e:
        print("[UPLOAD EXCEL] Ошибка:")
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=400)

@router.get("/filters_excel")
def get_excel_filters(file_id: str = Query(...), sheet: str = Query(...)):
    sheets = get_dataframe(file_id)
    if sheets is None or not isinstance(sheets, dict):
        return JSONResponse(content={"error": "file_id не найден или неверный формат"}, status_code=400)

    df = sheets.get(sheet)
    if df is None:
        return JSONResponse(content={"error": f"Лист '{sheet}' не найден"}, status_code=400)

    filters = {
        "regions": sorted(df["регион"].dropna().unique().tolist()) if "регион" in df else [],
        "sheets": sorted(sheets.keys()),
    }

    # определяем даты автоматически
    if "дата начала строительства" in df:
        filters["min_start_date"] = str(df["дата начала строительства"].min().date())
        filters["max_start_date"] = str(df["дата начала строительства"].max().date())

    if "дата завершения 2/дата по апоэ" in df:
        filters["min_end_date"] = str(df["дата завершения 2/дата по апоэ"].min().date())
        filters["max_end_date"] = str(df["дата завершения 2/дата по апоэ"].max().date())

    return filters

@router.post("/analyze_excel")
async def analyze_excel(file_id: str = Form(...), filters: str = Form("{}")):
    try:
        sheets = get_dataframe(file_id)
        if sheets is None or not isinstance(sheets, dict):
            return JSONResponse(content={"error": "file_id не найден или неверный формат"}, status_code=400)

        filters_dict = json.loads(filters)
        print(f"[ANALYZE EXCEL] filters: {filters_dict}")

        sheet_name = filters_dict.get("sheet")
        if not sheet_name or sheet_name not in sheets:
            return JSONResponse(content={"error": "Укажите корректный лист Excel"}, status_code=400)

        df = sheets[sheet_name]
        df = apply_filters(df, filters_dict)
        print(f"[ANALYZE EXCEL] строк после фильтрации: {len(df)}")

        summary = compute_summary(df)
        return JSONResponse(content=json.loads(json.dumps(summary, allow_nan=False)), status_code=200)

    except Exception as e:
        print("[ANALYZE EXCEL] Ошибка:")
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=400)
