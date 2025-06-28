from typing import Any
from fastapi import APIRouter, Query, UploadFile, File, Form
from fastapi.responses import JSONResponse
import pandas as pd
from io import StringIO
import json
import traceback

from services.csv_parser import clean_dataframe
from usecases.csv_analyze_deals import apply_filters, compute_summary, find_column

router = APIRouter()
cached_df: pd.DataFrame | None = None  # Глобальный кэш

@router.post("/upload")
async def upload_csv(csv_file: UploadFile = File(...)):
    global cached_df
    try:
        print("[UPLOAD] Загружаем файл...")
        content = (await csv_file.read()).decode("utf-8")
        df = pd.read_csv(
            StringIO(content),
            sep=";",
            encoding="utf-8",
            quotechar='\"',
            escapechar='\\\\',
            doublequote=False,
            engine="python",
            on_bad_lines="warn"
        )
        df.columns = df.columns.str.strip()
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(r'\\"', '\"', regex=True)
                df[col] = df[col].str.replace(r'\\\\', '', regex=True)

        cached_df = clean_dataframe(df)
        print(f"[UPLOAD] Загружено строк: {len(cached_df)}")
        return {"status": "ok"}

    except Exception as e:
        print(f"[UPLOAD] Ошибка: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@router.get("/filters")
def get_available_filters(region_col: str = Query(None)):
    global cached_df
    if cached_df is None:
        return JSONResponse(content={"error": "Файл не загружен"}, status_code=400)

    df = cached_df
    region_columns = [col for col in df.columns if "Регион" in col]
    selected_col = region_col if region_col in df.columns else find_column(df, "Регион")
    deal_type_col = next((col for col in df.columns if col.strip() == "Тип сделки"), None)

    return {
        "regions": sorted(df[selected_col].dropna().unique().tolist()) if selected_col else [],
        "region_columns": region_columns,
        "statuses": sorted(df["Текущий статус"].dropna().unique().tolist()) if "Текущий статус" in df else [],
        "stages": sorted(df["Стадия сделки"].dropna().unique().tolist()) if "Стадия сделки" in df else [],
        "responsibles": sorted(df["Ответственный"].dropna().unique().tolist()) if "Ответственный" in df else [],
        "funnels": sorted(df["Воронка"].dropna().unique().tolist()) if "Воронка" in df else [],
        "deal_types": sorted(df[deal_type_col].dropna().unique().tolist()) if deal_type_col else [],
    }

@router.post("/analyze")
async def analyze(filters: str = Form("{}")):
    global cached_df
    try:
        if cached_df is None:
            return JSONResponse(content={"error": "Файл не загружен"}, status_code=400)

        filters_dict = json.loads(filters)
        print(f"[ANALYZE] filters: {filters_dict}")

        df = apply_filters(cached_df, filters_dict)
        print(f"[ANALYZE] строк после фильтрации: {len(df)}")

        region_col = filters_dict.get("region_col")
        if not region_col or region_col not in df.columns or df[region_col].isna().all():
            region_col = "Регион (гарантирование)" if "Регион (гарантирование)" in df.columns else None

        summary = compute_summary(df, filters_dict, region_col)
        return JSONResponse(content=json.loads(json.dumps(summary, allow_nan=False)), status_code=200)

    except Exception as e:
        print("[ANALYZE] Ошибка:")
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=400)
