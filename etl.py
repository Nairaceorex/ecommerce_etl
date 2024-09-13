import pandas as pd
from sqlalchemy import create_engine
from config import DB_PATH, TRANSACTIONS_FILE
import logging

logging.basicConfig(
    filename='ecommerce_etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def extract():
    logging.info('Начало извлечения данных')
    try:
        data = pd.read_csv(TRANSACTIONS_FILE)
        logging.info('Извлечение данных завершено успешно')
        return data
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных: {e}")
        raise


def transform(data):
    logging.info('Начало трансформации данных')
    try:
        data['transaction_date'] = pd.to_datetime(data['transaction_date'])
        data['total'] = data['price'] * data['quantity']
        logging.info('Трансформация данных завершена успешно')
        return data
    except Exception as e:
        logging.error(f"Ошибка при трансформации данных: {e}")
        raise


def load(data):
    logging.info('Начало загрузки данных')
    try:
        engine = create_engine(f'sqlite:///{DB_PATH}', echo=False)
        data.to_sql('transactions', con=engine, if_exists='replace', index=False)
        logging.info('Загрузка данных завершена успешно')
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных: {e}")
        raise


def run_etl():
    try:
        data = extract()
        transformed_data = transform(data)
        load(transformed_data)
        logging.info("ETL процесс завершен успешно")
        print("ETL процесс завершен успешно.")
    except Exception as e:
        logging.critical(f"Критическая ошибка в процессе ETL: {e}")


if __name__ == '__main__':
    run_etl()
