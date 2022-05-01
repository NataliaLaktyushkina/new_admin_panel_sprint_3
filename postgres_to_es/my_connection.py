import logging
import os
import time

import psycopg2

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from functools import wraps
from get_data_psql import load_from_psql
from psycopg2.extras import DictCursor


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except ConnectionError:
                    if t >= border_sleep_time:
                        t = border_sleep_time
                    else:
                        t = start_sleep_time * 2 ^ factor
                    time.sleep(t)
        return inner

    return func_wrapper


def get_environment_var():
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
    env_var = {'db_name': os.environ.get('DB_NAME'), 'user': os.environ.get('USER_APP'),
               'password': os.environ.get('PASSWORD'), 'host': os.environ.get('HOST'), 'port': os.environ.get('PORT'),
               'db_path': os.environ.get('DB_PATH'),
               'elastic_pass': os.environ.get('ELASTIC_PASSWORD'),
               }

    return env_var


@backoff()
def connect_to_db():
    env_var = get_environment_var()

    dsl = {'dbname': env_var['db_name'],
           'user': env_var['user'],
           'password': env_var['password'],
           'host': env_var['host'],
           'port': env_var['port']}

    logging.basicConfig(filename='loading.log', filemode='w')
    logging.root.setLevel(logging.NOTSET)

    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_psql(pg_conn)
    pg_conn.close()


@backoff()
def connect_to_esl():
    elast_pass = get_environment_var()['elastic_pass']
    es = Elasticsearch("http://127.0.0.1:9200",
                       basic_auth=("elastic", elast_pass)
                       )
    logging.info(es.info())
    return es
