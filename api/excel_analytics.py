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
        cleaned_sheets = {}

        for sheet_name, df in sheet_dfs.items():
            df_cleaned = clean_excel_dataframe(df)
            cleaned_sheets[sheet_name] = df_cleaned # каждый лист отдельно

        file_id = store_dataframe(cleaned_sheets)  # сохраняем как dict
        print(f"[UPLOAD EXCEL] Загружено листов: {len(cleaned_sheets)}, file_id: {file_id}")
        return {"status": "ok", "file_id": file_id}
    except Exception as e:
        print("[UPLOAD EXCEL] Ошибка:")
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=400)

@router.get("/filters_excel")
def get_excel_filters(file_id: str, sheet: str = ""):
    sheets = get_dataframe(file_id)
    if sheets is None or not isinstance(sheets, dict):
        return JSONResponse(content={"error": "file_id не найден или неверный формат"}, status_code=400)

    sheet_names = sorted(sheets.keys())
    selected_df = sheets.get(sheet_names[0]) if not sheet else sheets.get(sheet)

    if selected_df is None:
        return JSONResponse(content={"error": f"Лист '{sheet}' не найден"}, status_code=400)

    region_col = selected_df.get("регион", pd.Series(dtype=str))

    if isinstance(region_col, pd.DataFrame):
        region_col = region_col.iloc[:, 0]  # берём первый из дубликатов

    region_col = region_col.dropna().astype(str)
    regions = sorted(region_col.unique().tolist())


    return {
        "regions": regions,
        "sheets": sheet_names,
        "start_year": (int(selected_df["дата начала строительства"].dt.year.min()) if "дата начала строительства" in selected_df else None),
        "end_year": (int(selected_df["дата завершения 2/дата по апоэ"].dt.year.max()) if "дата завершения 2/дата по апоэ" in selected_df else None),
    }

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

        region_col = "регион" if "регион" in df.columns else None
        summary = compute_summary(df, filters_dict, region_col)
        return JSONResponse(content=json.loads(json.dumps(summary, allow_nan=False)), status_code=200)

    except Exception as e:
        print("[ANALYZE EXCEL] Ошибка:")
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=400)
