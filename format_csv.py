import csv
from pathlib import Path

def format_csv_with_quotes(input_path, output_path):
    """
    Читает CSV файл и создает новый, где каждая ячейка обернута в двойные кавычки.
    
    Args:
        input_path: Путь к исходному CSV файлу
        output_path: Путь для сохранения отформатированного CSV файла
    """
    # Читаем исходный файл
    with open(input_path, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')
        rows = list(reader)
    
    # Записываем в новый файл с кавычками вокруг каждой ячейки
    with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(
            outfile, 
            delimiter=';',
            quoting=csv.QUOTE_ALL  # Заключать все поля в кавычки
        )
        writer.writerows(rows)
    
    print(f"Отформатированный файл сохранен: {output_path}")

if __name__ == "__main__":
    # Пути к файлам
    input_path = Path(__file__).parent.parent / "uploads" / "sample.csv"
    output_path = Path(__file__).parent.parent / "uploads" / "sample_formatted.csv"
    
    # Форматируем CSV
    format_csv_with_quotes(input_path, output_path)