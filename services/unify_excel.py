from typing import Dict
import pandas as pd
import re

KEYWORDS = [
    "Застройщик", "Стоимость", "Площадь", "Год", "Регион", "БИН"
]

def normalize_column(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = name.strip().lower().replace("\n", " ")
    name = re.sub(r"[\s\-]+", " ", name)
    if "стоимость" in name:
        return "стоимость"
    if "площадь" in name:
        return "площадь"
    if "регион" in name:
        return "регион"
    if "год" in name:
        return "год"
    if "бин" in name:
        return "бин"
    if "застройщик" in name:
        return "застройщик"
    return name

def find_column_by_keywords(columns, keyword: str) -> str | None:
    for col in columns:
        if re.search(rf"\b{keyword}\b", col.lower()):
            return col
        
    return None

def clean_excel_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Приведение колонок к нужным типам
    if "год" in df.columns:
        df["год"] = pd.to_numeric(df["год"], errors="coerce").astype("Int64")

    if "стоимость" in df.columns and isinstance(df["стоимость"], pd.Series):
        df["стоимость"] = pd.to_numeric(df["стоимость"], errors="coerce")

    if "площадь" in df.columns:
        df["площадь"] = pd.to_numeric(df["площадь"], errors="coerce")

    if "регион" in df.columns:
        df["регион"] = df["регион"].astype(str).str.strip()

    # Безопасное удаление строк без застройщика и стоимости
    builder_col = find_column_by_keywords(df.columns, "застройщик")
    cost_col = find_column_by_keywords(df.columns, "стоимость")

    cols_to_check = [col for col in [builder_col, cost_col] if col]
    if cols_to_check:
        df = df.dropna(subset=cols_to_check, how="all")

    return df

def parse_excel(file) -> Dict[str, pd.DataFrame]:
    xls = pd.ExcelFile(file)
    sheets_data = {}

    for sheet in xls.sheet_names:
        df_raw = pd.read_excel(xls, sheet_name=sheet, header=None)
        header_idx = df_raw[df_raw.apply(
            lambda row: row.astype(str).str.contains("Застройщик", case=False).any(),
            axis=1
        )].index

        if not header_idx.empty:
            header_row = header_idx[0]
            df = pd.read_excel(xls, sheet_name=sheet, skiprows=header_row, header=0)
            df.columns = [normalize_column(col) for col in df.columns]

            df["__source_sheet"] = sheet
            sheets_data[sheet] = df

    if not sheets_data:
        raise ValueError("Не удалось найти таблицы сделок в Excel.")

    return sheets_data
