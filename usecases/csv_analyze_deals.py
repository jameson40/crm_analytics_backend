
import pandas as pd
import json
from typing import Dict, Any

def find_column(df: pd.DataFrame, prefix: str) -> str | None:
    for col in df.columns:
        if col.strip().startswith(prefix):
            return col
    return None

def apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    filtered_df = df.copy()

    region_col = filters.get("region_col") or find_column(df, "Регион")

    deal_type_col = next((col for col in df.columns if col.strip() == "Тип сделки"), "Тип сделки")

    COLUMN_MAP = {
        "region": region_col or "Регион",
        "status": "Текущий статус",
        "stage": "Стадия сделки",
        "responsible": "Ответственный",
        "company": "Компания",
        "amount_min": "Сумма",
        "amount_max": "Сумма",
        "repeats": "Повторная сделка",
        "recontacts": "Повторное обращение",
        "from": "Дата создания",
        "to": "Дата завершения",
        "funnel": "Воронка",
        "deal_type": deal_type_col,
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

def compute_summary(df: pd.DataFrame, filters: Dict[str, Any], region_col: str | None = None) -> dict:
    return {
        "total_deals": int(len(df)),
        "total_amount": float(df["Сумма"].sum(skipna=True)) if "Сумма" in df and not df["Сумма"].isna().all() else 0.0,
        "avg_amount": float(df["Сумма"].mean(skipna=True)) if "Сумма" in df and not df["Сумма"].isna().all() else 0.0,
        "unique_companies": int(df["Компания"].nunique()) if "Компания" in df else 0,
        "deals_by_stage": df["Стадия сделки"].value_counts().to_dict() if "Стадия сделки" in df else {},
        "deals_by_status": df["Текущий статус"].value_counts().to_dict() if "Текущий статус" in df else {},
        "top_companies_by_sum": df.groupby("Компания")["Сумма"].sum().nlargest(5).to_dict() if "Компания" in df and "Сумма" in df else {},
        "top_companies_by_count": df["Компания"].value_counts().nlargest(5).to_dict() if "Компания" in df else {},
        "top_regions_by_sum": df.groupby(region_col)["Сумма"].sum().nlargest(5).to_dict() if region_col in df.columns and "Сумма" in df else {},
        "top_regions_by_sum_note": {"region_col": region_col} if region_col else None,
        "repeats": int((df["Повторная сделка"] == "Y").sum()) if "Повторная сделка" in df else 0,
        "recontacts": int((df["Повторное обращение"] == "Y").sum()) if "Повторное обращение" in df else 0,
        "deals_by_funnel": df["Воронка"].value_counts().to_dict() if "Воронка" in df else {},
    }
