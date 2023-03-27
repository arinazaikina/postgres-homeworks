import os

from psycopg2 import IntegrityError

from database import PostgresDatabase
from logger import logger
from utils import fill_table

if __name__ == '__main__':
    db = PostgresDatabase(dbname='north', user='postgres', password='0000', host='localhost', port='5432')

    try:
        db.connect()
        logger.info(f'Успешное подключение к базе данных {db.dbname}')
    except Exception as error:
        logger.error(error)

    try:
        db.create_table(table_name='employees', columns=[
            'employees_id serial PRIMARY KEY',
            'first_name varchar(100) NOT NULL',
            'last_name varchar(100) NOT NULL',
            'title varchar(100) NOT NULL',
            'birth_date date NOT NULL',
            'notes text'
        ])
        logger.info('Создана таблица employees')
    except IntegrityError as error:
        logger.warning(error)

    try:
        db.create_table(table_name='customers', columns=[
            'customer_id varchar(10) PRIMARY KEY',
            'company_name varchar(100)',
            'contact_name varchar(100)'
        ])
        logger.info('Создана таблица customers')
    except IntegrityError as error:
        logger.warning(error)

    try:
        db.create_table(table_name='orders', columns=[
            'order_id int PRIMARY KEY',
            'customer_id varchar(10) REFERENCES customers(customer_id)',
            'employee_id int REFERENCES employees(employees_id)',
            'order_date date NOT NULL',
            'ship_city varchar(100) NOT NULL'
        ])
        logger.info('Создана таблица orders')
    except IntegrityError as error:
        logger.warning(error)

    fill_table(
        path_to_file=os.path.join('north_data', 'employees_data.csv'),
        db=db,
        table_name='employees'
    )

    fill_table(
        path_to_file=os.path.join('north_data', 'customers_data.csv'),
        db=db,
        table_name='customers'
    )

    fill_table(
        path_to_file=os.path.join('north_data', 'orders_data.csv'),
        db=db,
        table_name='orders'
    )

    db.disconnect()
    logger.info(f'Отключение от базы дынных {db.dbname}')
