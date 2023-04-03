from typing import Optional, List

import psycopg2
from psycopg2 import IntegrityError
from psycopg2.extensions import connection


class PostgresDatabase:
    """
    Базовый класс, описывающий базу данных
    Attr:
        dbname (str): название базы данных
        user (str): логин пользователя
        password (str): пароль
        host (str): имя или IP-адрес машины, на которой запущена база данных
        port (str): номер порта, на котором запущена база данных
        connection: соединение с базой данных (по умолчанию None)
    """

    def __init__(self, dbname: str, user: str, password: str, host: str, port: int = 5432) -> None:
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection: Optional[connection] = None

    def connect(self) -> None:
        """Устанавливает соединение с базой данных"""
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port
            )
        except psycopg2.OperationalError as error:
            raise Exception(f'Не удалось подключиться к базе данных: {error}')

    def execute(self, query: str, params=None, fetchall=False, fetchmany=None, fetchone=False, commit=False):
        """
        Выполняет SQL-запрос к базе данных

        :param query: SQL команда
        :param params: параметры SQL запроса (по умолчанию None)
        :param fetchall: вернуть все строки результата (по умолчанию False)
        :param fetchmany: вернуть несколько строк результата (по умолчанию None)
        :param fetchone: вернуть только одну строку результата (по умолчанию False)
        :param commit: выполнить коммит транзакции после выполнения запроса
        (False - изменения не будут сохранены, True - изменения в базе данных будут сохранены, по умолчанию False)
        """

        cursor: psycopg2.extensions.cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)

            if commit:
                self.connection.commit()
            if fetchone:
                result = cursor.fetchone()
            elif fetchall:
                result = cursor.fetchall()
            elif fetchmany is not None:
                result = cursor.fetchmany(size=fetchmany)
            else:
                result = None

            return result

        except psycopg2.Error as error:
            raise Exception(f'Ошибка выполнения запроса: {error}')

        finally:
            cursor.close()

    def disconnect(self) -> None:
        """Отключает соединение с базой данных"""
        if self.connection:
            self.connection.close()

    def has_table(self, table_name: str) -> bool:
        query = f"SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = '{table_name}')"
        result = self.execute(query, fetchone=True)
        return result[0]

    def create_table(self, table_name: str, columns: List[str]) -> None:
        """
        Создаёт таблицу в базе данных

        :param table_name: название таблицы
        :param columns: список столбцов
        """

        if not self.has_table(table_name):
            query = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(columns)})'
            self.execute(query=query, commit=True)
        else:
            raise IntegrityError(f'Таблица {table_name} уже существует')

    def record_exists(self, table_name: str, columns: List[str], data: tuple) -> bool:
        """
        Проверяет существует ли запись в базе данных.
        Возвращает True, если запись существует.
        В противном случае возвращает False.
        :param table_name: название таблицы
        :param columns: список названий столбцов для условия поиска
        :param data: кортеж значений для условия поиска
        """
        query = f"SELECT * FROM {table_name} WHERE "

        where_conditions = []
        for column in columns:
            where_conditions.append(f"{column} = %s")
        query += " AND ".join(where_conditions)

        result = self.execute(query=query, params=data, fetchone=True)

        if result is None:
            return False
        return True

    def insert(self, table_name: str, columns: List[str], data: tuple) -> None:
        """
        Добавляет запись в таблицу
        :param table_name: название таблицы
        :param columns: список, имён столбцов, в которые добавляются данные
        :param data: данные, которые надо добавить в таблицу
        """

        if not self.record_exists(table_name=table_name, columns=columns, data=data):
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
            self.execute(query=query, params=data, commit=True)
        else:
            raise IntegrityError(f'В таблице {table_name} уже существует запись c params={data}')
