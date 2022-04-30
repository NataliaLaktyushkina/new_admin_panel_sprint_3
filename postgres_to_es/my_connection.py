import logging
import os
import psycopg2

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from get_data_psql import load_from_psql
from psycopg2.extras import DictCursor


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


def connect_to_esl():
    elast_pass = get_environment_var()['elastic_pass']
    es = Elasticsearch("http://127.0.0.1:9200",
                       basic_auth=("elastic", elast_pass)
                       )
    logging.info(es.info())
    return es
