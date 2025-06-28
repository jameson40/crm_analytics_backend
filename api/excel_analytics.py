from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse
import pandas as pd
import traceback
import json

from services.unify_excel import parse_excel_unified, clean_excel_dataframe
from usecases.excel_analyze_deals import apply_filters, compute_summary
from services.file_cache import store_dataframe, get_dataframe

router = APIRouter()

@router.post("/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    try:
        df = parse_excel_unified(file.file)
        df = clean_excel_dataframe(df)
        file_id = store_dataframe(df)
        print(f"[UPLOAD EXCEL] Загружено строк: {len(df)}, file_id: {file_id}")
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
        df = get_dataframe(file_id)
        if df is None:
            return JSONResponse(content={"error": "file_id не найден"}, status_code=400)

        filters_dict = json.loads(filters)
        print(f"[ANALYZE EXCEL] filters: {filters_dict}")

        df = apply_filters(df, filters_dict)
        print(f"[ANALYZE EXCEL] строк после фильтрации: {len(df)}")

        region_col = "регион" if "регион" in df.columns else None
        summary = compute_summary(df, filters_dict, region_col)
        return JSONResponse(content=json.loads(json.dumps(summary, allow_nan=False)), status_code=200)

    except Exception as e:
        print("[ANALYZE EXCEL] Ошибка:")
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=400)
