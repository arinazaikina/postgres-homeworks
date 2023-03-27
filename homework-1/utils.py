import csv

from psycopg2 import IntegrityError

from database import PostgresDatabase
from logger import logger


def get_data_from_file(path_to_file: str) -> tuple:
    """
    Возвращает список с названиями полей и
    данные из файла csv в виде списка кортежей со значениями атрибутов
    сущности таблицы базы данных
    :param path_to_file: путь к файлу с данными
    """
    result = []
    try:
        with open(path_to_file, 'r', encoding='utf-8') as file:
            csv_file = csv.DictReader(file)
            for line in csv_file:
                result.append(tuple(line.values()))
    except FileNotFoundError as error:
        logger.error(error)
        raise FileNotFoundError(f'Не удалось найти файл {path_to_file}: {error}')

    logger.debug(f'Из файла {path_to_file} получены данные {result}')
    return csv_file.fieldnames, result


def fill_table(path_to_file: str, db: PostgresDatabase, table_name: str) -> None:
    """
    Заполняет таблицу в базе данных данными из файла
    :param path_to_file: путь к файлу
    :param db: экземпляр базы данных
    :param table_name: название таблицы
    """
    data = get_data_from_file(path_to_file)
    for item in data[1]:
        try:
            db.insert(table_name=table_name, data=item, columns=data[0])
            logger.info(f'В таблицу "{table_name}" добавлена запись c params={item}')
        except IntegrityError as error:
            logger.warning(error)
