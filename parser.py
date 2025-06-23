import pandas as pd
from pathlib import Path

def load_csv(file_path: str) -> pd.DataFrame:
    # Сначала попробуем прочитать с параметрами для обработки экранированных кавычек
    try:
        df = pd.read_csv(
            file_path, 
            sep=";",
            encoding="utf-8",
            engine='python',
            quotechar='"',
            escapechar='\\',  # Используем обратный слеш для экранирования
            doublequote=False,  # Не считаем двойные кавычки как одиночные
            on_bad_lines='warn'
        )
    except Exception as e:
        print(f"Ошибка при чтении CSV с экранированием: {e}")
        # Если не получилось, попробуем более простой подход
        df = pd.read_csv(
            file_path, 
            sep=";",
            encoding="utf-8",
            engine='python',
            quoting=3,  # QUOTE_NONE - игнорируем кавычки полностью
            on_bad_lines='warn'
        )
    
    # Очистка названий колонок от лишних пробелов
    df.columns = df.columns.str.strip()
    
    # Очистка строковых данных от экранирования
    for col in df.columns:
        if df[col].dtype == 'object':  # Только для строковых колонок
            # Заменяем экранированные кавычки на обычные
            df[col] = df[col].str.replace(r'\\"', '"', regex=True) if pd.api.types.is_string_dtype(df[col]) else df[col]
            # Удаляем лишние обратные слеши
            df[col] = df[col].str.replace(r'\\', '', regex=True) if pd.api.types.is_string_dtype(df[col]) else df[col]
    
    return df

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Приведение колонок с датами к datetime
    date_columns = [
        "Дата создания", "Дата изменения", "Дата начала",
        "Предполагаемая дата закрытия", "Дата регистрации заявления (субсидирование)"
    ]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%d.%m.%Y %H:%M:%S", errors="coerce")
            df[col] = df[col].fillna(pd.to_datetime(df[col], format="%d.%m.%Y", errors="coerce"))

    # Приведение числовых колонок к float
    float_columns = [
        "Сумма", "Стоимость незавершенного строительства (гарантирование)",
        "Площадь ЗУ, га (гарантирование)", "Цена реализации 1 кв.м жилья в тыс.тенге/1 м2 (гарантирование)"
    ]
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def parse_and_clean_csv(file_path: str) -> pd.DataFrame:
    df = load_csv(file_path)
    print(f"[parser] Загружено строк: {len(df)}")
    df = clean_dataframe(df)
    return df

if __name__ == "__main__":
    sample_path = Path(__file__).parent.parent / "uploads" / "sample.csv"
    df = parse_and_clean_csv(sample_path)
    print(df.head())