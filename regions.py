from io import StringIO
from fastapi import APIRouter, File, UploadFile
import pandas as pd
from pathlib import Path

router = APIRouter()

UPLOADS_DIR = Path(__file__).parent.parent / "uploads"
LATEST_FILE = UPLOADS_DIR / "sample.csv"

@router.get("/regions")
def get_unique_regions():
    try:
        df = pd.read_csv(LATEST_FILE, sep=";", encoding="utf-8", quotechar='"')
        df.columns = df.columns.str.strip()

        possible_columns = ["Регион", "Регион (гарантирование)", "Регион (субсидирование)"]
        found_column = None

        for col in possible_columns:
            if col in df.columns:
                found_column = col
                break

        if not found_column:
            return {"error": "Регион колонка не найдена"}

        regions = df[found_column].dropna().unique()
        return {"regions": sorted(regions.tolist())}
    except Exception as e:
        return {"error": str(e)}

@router.post("/parse_regions")
async def parse_regions(csv_file: UploadFile = File(...)):
    try:
        # Читаем содержимое CSV
        content = (await csv_file.read()).decode("utf-8")
        df = pd.read_csv(StringIO(content), sep=";", quotechar='"', engine="python")
        df.columns = df.columns.str.strip()

        # Ищем колонку с регионом
        region_columns = [col for col in df.columns if "регион" in col.lower()]
        if not region_columns:
            return {"regions": []}

        # Собираем уникальные значения из всех найденных колонок
        regions = set()
        for col in region_columns:
            regions.update(df[col].dropna().astype(str).str.strip().unique())

        return {"regions": sorted(regions)}

    except Exception as e:
        return {"error": str(e)}
