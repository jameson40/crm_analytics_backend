from fastapi import APIRouter, UploadFile, File, Form, Query
from fastapi.responses import JSONResponse
from io import StringIO
import pandas as pd
import json
import traceback

from models.models import AnalyzeCsvRequest, FiltersResponse, UploadResponse
from services.csv_parser import clean_dataframe
from usecases.csv_analyze_deals import apply_filters, compute_summary, find_column
from services.file_cache import store_dataframe, get_dataframe

router = APIRouter()

@router.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...)):
    try:
        content = (await file.read()).decode("utf-8")
        df = pd.read_csv(
            StringIO(content),
            sep=";",
            encoding="utf-8",
            quotechar='"',
            escapechar='\\',
            doublequote=False,
            engine="python",
            on_bad_lines="warn"
        )
        df.columns = df.columns.str.strip()
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(r'\\"', '"', regex=True)
                df[col] = df[col].str.replace(r'\\', '', regex=True)

        df = clean_dataframe(df)
        file_id = store_dataframe(df)
        print(f"[UPLOAD] CSV загружен, строк: {len(df)}, file_id: {file_id}")
        
        return UploadResponse(status="ok", file_id=file_id)

    except Exception as e:
        print(f"[UPLOAD] Ошибка: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@router.get("/filters_csv", response_model=FiltersResponse)
def get_available_filters(file_id: str, region_col: str = Query(None)):
    try:
        df = get_dataframe(file_id)
        print(f"[FILTERS] Получен запрос на фильтры для file_id: {file_id}")
        if df is None:
            return JSONResponse(content={"error": "file_id не найден"}, status_code=400)

        print(f"[FILTERS] Строк: {len(df)}")

        region_columns = [col for col in df.columns if "Регион" in col]
        selected_col = region_col if region_col in df.columns else find_column(df, "Регион")
        deal_type_col = next((col for col in df.columns if col.strip() == "Тип сделки"), None)

        print("[FILTERS] Выбранная колонка региона:", selected_col)

        result = FiltersResponse(
            regions=sorted(df[selected_col].dropna().unique().tolist()) if selected_col else [],
            region_columns=region_columns,
            statuses=sorted(df["Текущий статус"].dropna().unique().tolist()) if "Текущий статус" in df else [],
            stages=sorted(df["Стадия сделки"].dropna().unique().tolist()) if "Стадия сделки" in df else [],
            responsibles=sorted(df["Ответственный"].dropna().unique().tolist()) if "Ответственный" in df else [],
            funnels=sorted(df["Воронка"].dropna().unique().tolist()) if "Воронка" in df else [],
            deals_type=sorted(df[deal_type_col].dropna().unique().tolist()) if deal_type_col else [],
        )

        return result
    except Exception as e:
        print(f"[FILTERS] Ошибка: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@router.get("/regions_csv")
def get_regions_csv(file_id: str, region_col: str = Query(...)):
    df = get_dataframe(file_id)
    if df is None or region_col not in df.columns:
        return JSONResponse(content={"regions": []}, status_code=200)
    regions = sorted(df[region_col].dropna().unique().tolist())
    return {"regions": regions}

@router.post("/analyze_csv")
async def analyze_csv(payload: AnalyzeCsvRequest):
    file_id = payload.file_id
    filters = payload.filters
    try:
        df = get_dataframe(file_id)
        if df is None:
            return JSONResponse(content={"error": "file_id не найден"}, status_code=400)

        filters_dict = filters
        print(f"[ANALYZE CSV] filters: {filters_dict}")

        df = apply_filters(df, filters_dict)
        print(f"[ANALYZE CSV] строк после фильтрации: {len(df)}")

        region_col = filters_dict.get("region_col")
        if not region_col or region_col not in df.columns or df[region_col].isna().all():
            region_col = "Регион (гарантирование)" if "Регион (гарантирование)" in df.columns else None

        summary = compute_summary(df, filters_dict, region_col)
        return JSONResponse(content=json.loads(json.dumps(summary, allow_nan=False)), status_code=200)

    except Exception as e:
        print("[ANALYZE CSV] Ошибка:")
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=400)
