import pandas as pd
from typing import Dict, Any

def apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    filtered_df = df.copy()

    if "regions" in filters and "регион" in df.columns:
        filtered_df = filtered_df[filtered_df["регион"].isin(filters["regions"])]

    if "sheets" in filters and "__source_sheet" in df.columns:
        filtered_df = filtered_df[filtered_df["__source_sheet"].isin(filters["sheets"])]

    if "years" in filters and "год" in df.columns:
        filtered_df = filtered_df[filtered_df["год"].isin(filters["years"])]

    if "min_cost" in filters and "стоимость" in df.columns:
        try:
            min_cost = float(filters["min_cost"])
            filtered_df = filtered_df[filtered_df["стоимость"] >= min_cost]
        except ValueError:
            pass

    if "max_cost" in filters and "стоимость" in df.columns:
        try:
            max_cost = float(filters["max_cost"])
            filtered_df = filtered_df[filtered_df["стоимость"] <= max_cost]
        except ValueError:
            pass

    return filtered_df

def compute_summary(df: pd.DataFrame) -> dict:
    return {
        "total_rows": int(len(df)),
        "total_cost": float(df["стоимость"].sum(skipna=True)) if "стоимость" in df else 0.0,
        "total_area": float(df["площадь"].sum(skipna=True)) if "площадь" in df else 0.0,
        "avg_cost": float(df["стоимость"].mean(skipna=True)) if "стоимость" in df else 0.0,
        "by_region": df["регион"].value_counts().to_dict() if "регион" in df else {},
        "by_sheet": df["__source_sheet"].value_counts().to_dict() if "__source_sheet" in df else {},
        "by_year": df["год"].value_counts().to_dict() if "год" in df else {},
        "top_builders_by_cost": df.groupby("застройщик")["стоимость"].sum().nlargest(5).to_dict()
            if "застройщик" in df and "стоимость" in df else {}
    }
