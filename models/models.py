from typing import List, Optional, Dict, Any
from pydantic import BaseModel

# Запрос на анализ CSV
class AnalyzeCsvRequest(BaseModel):
    file_id: str
    filters: Dict[str, Any]

# Запрос на анализ Excel
class AnalyzeExcelRequest(BaseModel):
    file_id: str
    filters: Dict[str, Any]
    sheet_name: Optional[str] = None

# Ответ при загрузке файла
class UploadResponse(BaseModel):
    status: str
    file_id: str

# Ответ с доступными фильтрами
class FiltersResponse(BaseModel):
    regions: List[str]
    region_columns: List[str]
    statuses: List[str]
    stages: List[str]
    responsibles: List[str]
    funnels: List[str]
    deals_type: List[str]
