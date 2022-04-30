import datetime
from dateutil.parser import parse
from elasticsearch import helpers
from psycopg2.extensions import connection as _connection

import my_connection
import json
from my_connection import *

from state import *

BULK = 1000
fp = JsonFileStorage(os.path.dirname(os.path.abspath(__file__)))
state = State(fp)


def create_tables_list():
    tables_list = list()
    tables_list.append('film_work')

    return tables_list


def check_state():
    current_state = state.get_state('modified')
    if current_state is None:
        current_state = datetime.datetime.now()
        state.set_state('modified', current_state.isoformat())
    return current_state


def get_data_from_table(pg_conn:_connection, table:str):

    if table == 'film_work':
        try:
            with pg_conn.cursor() as p_curs:

                modified = check_state()

                query_text = """
                    SELECT json_agg(movies_data)
                    FROM (
                        SELECT
                            fw.id as _id,
                            fw.id,
                            fw.title,
                            fw.description,
                            fw.rating as imdb_rating,
                            COALESCE(
                                json_agg(
                                    DISTINCT p.full_name
                                    ) FILTER (WHERE pfw.role = 'director'),
                                '[]')
                            as director,
                            COALESCE (
                                json_agg(
                                    DISTINCT jsonb_build_object(
                                   'id', p.id,
                                   'name', p.full_name
                                    )
                                ) FILTER (WHERE p.id is not null AND pfw.role = 'actor'),
                                '[]'
                            ) as actors,
                            COALESCE (
                                json_agg(
                                    DISTINCT p.full_name
                                    ) FILTER (WHERE p.id is not null AND pfw.role = 'actor'),
                                '[]'
                            ) as actors_names,
                            COALESCE (
                                json_agg(
                                    DISTINCT jsonb_build_object(
                                   'id', p.id,
                                   'name', p.full_name
                                    )
                                ) FILTER (WHERE p.id is not null AND pfw.role = 'writer'),
                                '[]'
                            ) as writers,
                            COALESCE (
                                json_agg(
                                    DISTINCT p.full_name
                                    ) FILTER (WHERE p.id is not null AND pfw.role = 'writer'),
                                '[]'
                            ) as writers_names,
                            array_agg(DISTINCT g.name) as genre
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        LEFT JOIN content.person p ON p.id = pfw.person_id
                        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                        LEFT JOIN content.genre g ON g.id = gfw.genre_id
                        WHERE fw.updated_at > %s         
                        GROUP BY fw.id
                        ORDER BY fw.updated_at
                        LIMIT %s ) 
                    movies_data;"""

                p_curs.execute(query_text, (modified, BULK,))
                data = p_curs.fetchall()
                with open('movies.json', 'w') as movies_data:
                    json.dump(data[0][0], movies_data)
                send_data_to_es()

        except psycopg2.Error as error:
            logging.error('Ошибка чтения таблицы ', table, error)


def send_data_to_es():
    es_client = my_connection.connect_to_esl()
    if es_client.ping():
        index_name = 'movies'
        index_created = create_index(es_client, index_name)
        if index_created:
            try:
                with open('movies.json', 'r') as movies_data:
                    bulk_data = json.load(movies_data)
                response = helpers.bulk(es_client, bulk_data, index=index_name)
                logging.info(' '.join(('Bulk', str(response[0]), 'documents')))
                state.set_state('modified', datetime.datetime.now().isoformat())
            except Exception as e:
                logging.error(e.args[0])


def create_index(es_object, index_name):
    created = False
    with open('index_settings.json', 'r') as ind_set:
        settings = json.load(ind_set)
    try:
        if not es_object.indices.exists(index=index_name):
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            logging.info(' '.join(('Created Index', index_name)))
        created = True
    except Exception as ex:
        logging.error(ex)
    finally:
        return created


def load_from_psql(pg_conn: _connection):
    tables_list = create_tables_list()
    for table_name in tables_list:
        get_data_from_table(pg_conn, table_name)


if __name__ == '__main__':
    connect_to_db()