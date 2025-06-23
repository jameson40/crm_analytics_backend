from typing import Any, Dict
from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse
import pandas as pd
from pandas.api.types import is_numeric_dtype
from io import StringIO
import json

from parser import clean_dataframe

router = APIRouter()
cached_df: pd.DataFrame | None = None  # Глобальный кэш
selected_region_column: str | None = None  # Новая глобальная переменная для выбранной колонки региона

# Ищет первую колонку по префиксу
def find_column(df: pd.DataFrame, prefix: str) -> str | None:
    for col in df.columns:
        if col.strip().startswith(prefix):
            return col
    return None

def apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    filtered_df = df.copy()

    region_col = filters.get("region_col") or find_column(df, "Регион")
    print("[DEBUG] Колонка региона:", region_col)
    print("[DEBUG] Значения региона:", df[region_col].dropna().unique())

    COLUMN_MAP = {
        "region": region_col or "Регион",
        "status": "Текущий статус",
        "stage": "Стадия сделки",
        "responsible": "Ответственный",
        "company": "Компания",
        "amount_min": "Сумма",
        "amount_max": "Сумма",
        "повторная_сделка": "Повторная сделка",
        "повторное_обращение": "Повторное обращение",
        "from": "Дата создания",
        "to": "Дата создания"
    }

    for key, value in filters.items():
        if key == "region_col":
            continue

        column = COLUMN_MAP.get(key)
        if column not in filtered_df.columns or value in [None, '', [], {}]:
            continue

        if key == "from":
            filtered_df = filtered_df[filtered_df[column] >= pd.to_datetime(value, errors="coerce")]
        elif key == "to":
            filtered_df = filtered_df[filtered_df[column] <= pd.to_datetime(value, errors="coerce")]
        elif key in {"amount_min", "amount_max"}:
            try:
                val = float(value)
                if key == "amount_min":
                    filtered_df = filtered_df[filtered_df[column] >= val]
                else:
                    filtered_df = filtered_df[filtered_df[column] <= val]
            except ValueError:
                continue
        elif key in {"повторная_сделка", "повторное_обращение"}:
            filtered_df = filtered_df[filtered_df[column] == ("Y" if value else "N")]
        elif isinstance(value, list):
            filtered_df = filtered_df[filtered_df[column].isin(value)]
        else:
            filtered_df = filtered_df[filtered_df[column] == value]

    return filtered_df

def compute_summary(df: pd.DataFrame) -> dict:
    return {
        "total_deals": int(len(df)),
        "total_amount": float(df["Сумма"].sum(skipna=True)) if "Сумма" in df and not df["Сумма"].isna().all() else 0.0,
        "avg_amount": float(df["Сумма"].mean(skipna=True)) if "Сумма" in df and not df["Сумма"].isna().all() else 0.0,
        "unique_companies": int(df["Компания"].nunique()) if "Компания" in df else 0,
        "deals_by_stage": df["Стадия сделки"].value_counts().to_dict() if "Стадия сделки" in df else {},
        "deals_by_status": df["Текущий статус"].value_counts().to_dict() if "Текущий статус" in df else {},
        "top_companies_by_sum": df.groupby("Компания")["Сумма"].sum().nlargest(5).to_dict() if "Компания" in df and "Сумма" in df else {},
        "top_companies_by_count": df["Компания"].value_counts().nlargest(5).to_dict() if "Компания" in df else {},
        "repeats": int((df["Повторная сделка"] == "Y").sum()) if "Повторная сделка" in df else 0,
        "recontacts": int((df["Повторное обращение"] == "Y").sum()) if "Повторное обращение" in df else 0,
        "deals_by_voronka": df["Воронка"].value_counts().to_dict() if "Воронка" in df else {},
        "empty_amounts": int(df["Сумма"].isna().sum()) if "Сумма" in df else 0,
    }

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

        cached_df = clean_dataframe(df)

        print(f"[UPLOAD] Загружено строк: {len(cached_df)}")
        print(f"[UPLOAD] Колонки: {cached_df.columns.tolist()}")
        return {"status": "ok"}

    except Exception as e:
        print(f"[UPLOAD] Ошибка: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@router.get("/filters")
def get_available_filters():
    global cached_df

    if cached_df is None:
        print("[FILTERS] cached_df отсутствует!")
        return JSONResponse(content={"error": "Файл не загружен"}, status_code=400)

    df = cached_df
    print(f"[FILTERS] Строк: {len(df)}")
    print(f"[FILTERS] Колонки: {df.columns.tolist()}")

    region_col = find_column(df, "Регион")

    region_columns = [col for col in df.columns if "Регион" in col]

    return {
        "regions": sorted(df[region_col].dropna().unique().tolist()) if region_col else [],
        "region_columns": region_columns,
        "statuses": sorted(df["Текущий статус"].dropna().unique().tolist()) if "Текущий статус" in df else [],
        "stages": sorted(df["Стадия сделки"].dropna().unique().tolist()) if "Стадия сделки" in df else [],
        "responsibles": sorted(df["Ответственный"].dropna().unique().tolist()) if "Ответственный" in df else [],
    }

@router.post("/analyze")
async def analyze(filters: str = Form("{}")):
    global cached_df
    import traceback

    try:
        if cached_df is None:
            print("[ANALYZE] cached_df отсутствует!")
            return JSONResponse(content={"error": "Файл не загружен"}, status_code=400)

        filters_dict = json.loads(filters)
        print(f"[ANALYZE] filters: {filters_dict}")

        df = apply_filters(cached_df, filters_dict)
        print(f"[ANALYZE] строк после фильтрации: {len(df)}")

        summary = compute_summary(df)
        return JSONResponse(content=json.loads(json.dumps(summary, allow_nan=False)), status_code=200)

    except Exception as e:
        print("[ANALYZE] Ошибка:")
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=400)
