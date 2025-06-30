from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse
import pandas as pd
import traceback
import json

from services.unify_excel import parse_excel, clean_excel_dataframe
from usecases.excel_analyze_deals import apply_filters, compute_summary
from services.file_cache import store_dataframe, get_dataframe

router = APIRouter()

@router.post("/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    try:
        sheet_dfs = parse_excel(file.file)
        sheet_dfs_cleaned = {}

        for sheet, df in sheet_dfs.items():
            df_cleaned = clean_excel_dataframe(df)
            df_cleaned["__source_sheet"] = sheet
            sheet_dfs_cleaned[sheet] = df_cleaned

        file_id = store_dataframe(sheet_dfs_cleaned)
        total_rows = sum(len(df) for df in sheet_dfs_cleaned.values())
        print(f"[UPLOAD EXCEL] Загружено строк: {total_rows}, file_id: {file_id}")

        return {"status": "ok", "file_id": file_id}
    except Exception as e:
        print("[UPLOAD EXCEL] Ошибка:")
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=400)

@router.get("/filters_excel")
def get_excel_filters(file_id: str):
    df = get_dataframe(file_id)
    if df is None:
        return JSONResponse(content={"error": "file_id не найден"}, status_code=400)

    return {
        "regions": sorted(df["регион"].dropna().unique().tolist()) if "регион" in df else [],
        "sheets": sorted(df["__source_sheet"].dropna().unique().tolist()) if "__source_sheet" in df else [],
        "years": sorted(df["год"].dropna().unique().tolist()) if "год" in df else [],
    }

@router.post("/analyze_excel")
async def analyze_excel(file_id: str = Form(...), filters: str = Form("{}")):
    try:
        sheet_dfs = get_dataframe(file_id)
        if sheet_dfs is None:
            return JSONResponse(content={"error": "file_id не найден"}, status_code=400)

        filters_dict = json.loads(filters)
        print(f"[ANALYZE EXCEL] filters: {filters_dict}")

        selected_sheet = filters_dict.get("sheets", [None])[0]
        if not selected_sheet or selected_sheet not in sheet_dfs:
            return JSONResponse(content={"error": "Указанный лист не найден"}, status_code=400)

        df = sheet_dfs[selected_sheet]
        df = apply_filters(df, filters_dict)
        print(f"[ANALYZE EXCEL] строк после фильтрации: {len(df)}")

        region_col = "регион" if "регион" in df.columns else None
        summary = compute_summary(df, filters_dict, region_col)
        return JSONResponse(content=json.loads(json.dumps(summary, allow_nan=False)), status_code=200)

    except Exception as e:
        print("[ANALYZE EXCEL] Ошибка:")
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=400)

@router.post("/upload_excel_debug")
async def upload_excel_debug(file: UploadFile = File(...)):
    try:
        sheet_dfs = parse_excel(file.file)
        sheet_dfs_cleaned = {}

        for sheet, df in sheet_dfs.items():
            df_cleaned = clean_excel_dataframe(df)
            df_cleaned["__source_sheet"] = sheet
            sheet_dfs_cleaned[sheet] = df_cleaned

        # Соединяем все листы, чтобы сохранить в кэш (анализ делается по одному)
        combined_df = pd.concat(sheet_dfs_cleaned.values(), ignore_index=True)
        file_id = store_dataframe(combined_df)

        print(f"[UPLOAD EXCEL DEBUG] Загружено строк: {len(combined_df)}, file_id: {file_id}")

        return {
            "status": "ok",
            "file_id": file_id,
            "filename": file.filename,
            "content_type": file.content_type,
            "size": file.size,
        }
    except Exception as e:
        print("[UPLOAD EXCEL DEBUG] Ошибка:")
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=400)

