from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel

# Запрос на анализ CSV
class AnalyzeCsvRequest(BaseModel):
    file_id: str
    filters: Dict[str, Any]

class ExcelFilterRequest(BaseModel):
    file_id: str
    sheet_name: str

# Запрос на анализ Excel
class AnalyzeExcelRequest(BaseModel):
    file_id: str
    sheet_name: str
    filters: Dict[str, Union[
        List[str],             # select (region, developer)
        Dict[str, float],      # range (area)
        Dict[str, str]         # date_range (period)
    ]]

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
