import pandas as pd
from typing import Dict, Any

def apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    filtered_df = df.copy()

    if "regions" in filters and "регион" in df.columns:
        filtered_df = filtered_df[filtered_df["регион"].isin(filters["regions"])]

    if "start_date" in filters and "дата начала строительства" in df.columns:
        start = pd.to_datetime(filters["start_date"], errors="coerce")
        if not pd.isna(start):
            filtered_df = filtered_df[filtered_df["дата начала строительства"] >= start]

    if "end_date" in filters and "дата завершения 2/дата по апоэ" in df.columns:
        end = pd.to_datetime(filters["end_date"], errors="coerce")
        if not pd.isna(end):
            filtered_df = filtered_df[filtered_df["дата завершения 2/дата по апоэ"] <= end]

    return filtered_df

def compute_summary(df: pd.DataFrame) -> dict:
    return {
        "total_rows": int(len(df)),
        "total_cost": float(df["стоимость"].sum(skipna=True)) if "стоимость" in df else 0.0,
        "total_area": float(df["площадь"].sum(skipna=True)) if "площадь" in df else 0.0,
        "avg_cost": float(df["стоимость"].mean(skipna=True)) if "стоимость" in df else 0.0,
        "by_region": df["регион"].value_counts().to_dict() if "регион" in df else {},
        "by_sheet": df["__source_sheet"].value_counts().to_dict() if "__source_sheet" in df else {},
        "by_year": (
            df["дата начала строительства"].dt.year.value_counts().to_dict()
            if "дата начала строительства" in df and not df["дата начала строительства"].isna().all()
            else {}
        ),
        "top_builders_by_cost": df.groupby("застройщик")["стоимость"].sum().nlargest(5).to_dict()
            if "застройщик" in df and "стоимость" in df else {}
    }
