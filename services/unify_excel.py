
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

def parse_excel_unified(file) -> pd.DataFrame:
    xls = pd.ExcelFile(file)
    all_dfs = []

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
            df = df.dropna(subset=["застройщик", "стоимость"], how="all")
            all_dfs.append(df)

    if not all_dfs:
        raise ValueError("Не удалось найти таблицы сделок в Excel.")

    return pd.concat(all_dfs, ignore_index=True)
