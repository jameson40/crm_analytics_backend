from typing import Dict
import pandas as pd
import re

ALLOWED_SHEETS = [
    "Действующие", "Завершенные", "Отказ и расторжение", "Отозванные"
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
    if "регион" in name or "область" in name:
        return "регион"
    if "год" in name:
        return "год"
    if "бин" in name:
        return "бин"
    if "застройщик" in name:
        return "застройщик"
    if "дата начала строительства" in name:
        return "дата начала строительства"
    if "дата завершения" in name and "апоэ" in name:
        return "дата завершения 2/дата по апоэ"
    return name

def parse_excel(file) -> dict[str, pd.DataFrame]:
    xls = pd.ExcelFile(file)
    sheets_data = {}

    for sheet in xls.sheet_names:
        if sheet not in ALLOWED_SHEETS:
            continue

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

def find_column_by_keywords(columns, keyword: str) -> str | None:
    for col in columns:
        if re.search(rf"\b{keyword}\b", col.lower()):
            return col
    return None

def clean_excel_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if "дата начала строительства" in df.columns:
        df["дата начала строительства"] = pd.to_datetime(df["дата начала строительства"], errors="coerce")
    else:
        print("Колонка 'дата начала строительства' не найдена в Excel!")

    if "дата завершения 2/дата по апоэ" in df.columns:
        df["дата завершения 2/дата по апоэ"] = pd.to_datetime(
            df["дата завершения 2/дата по апоэ"].astype(str).str.extract(r"(\d{2}\.\d{2}\.\d{4})")[0],
            format="%d.%m.%Y",
            errors="coerce"
        )
    else: 
        print("Колонка 'дата завершения 2/дата по апоэ' не найдена в Excel!")

    if "Стоимость, тенге" in df.columns:
        df["Стоимость, тенге"] = pd.to_numeric(df["Стоимость, тенге"], errors="coerce")
    else:
        print("Колонка 'Стоимость, тенге' не найдена в Excel!")

    if "площадь" in df.columns:
        df["площадь"] = pd.to_numeric(df["площадь"], errors="coerce")
    else:
        print("Колонка 'площадь' не найдена в Excel!")

    if "регион" in df.columns:
        df["регион"] = df["регион"].astype(str).str.strip()
    else:
        print("Колонка 'регион' не найдена в Excel!")

    builder_col = find_column_by_keywords(df.columns, "застройщик")
    cost_col = find_column_by_keywords(df.columns, "стоимость")

    cols_to_check = [col for col in [builder_col, cost_col] if col]
    if cols_to_check:
        df = df.dropna(subset=cols_to_check, how="all")

    return df
