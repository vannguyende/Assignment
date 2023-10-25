from airflow.hooks.postgres_hook import PostgresHook
from pandas import DataFrame
import pandas as pd
import logging

logger = logging.getLogger('utils')


def get_pandas_df(postgres_conn_id, db_name, query):
    """
    Interact with Postgres to get the data with pandas dataframe format
    :param postgres_conn_id: id of postgres connection in airflow
    :param db_name: name of postgres db
    :param query: select query statement o execute
    :return: dataframe
    """
    pg_hook = PostgresHook(
        postgres_conn_id=postgres_conn_id,
        schema=db_name
    )

    df = pd.DataFrame()
    try:
        conn = pg_hook.get_conn()
    except Exception as err:
        print('Connection not configured properly.  Err: %s', err)
        return df

    if not conn:
        return df

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()
        df = DataFrame(records, columns=columns)
    except Exception as err:
        print('Database error.  %s', err)
    return df
