from airflow.decorators import dag, task
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
import os
import pandas as pd
from datetime import datetime, timedelta
from airflow.exceptions import AirflowSkipException
import logging
from common.utils import load_query, get_pandas_df

logger = logging.getLogger("airflow")

dag_id = "AssignmentSyncOnlineRetail"
tag = "assignment"
default_args = {'owner': 'vannh'}
sql = "assignment_online_retail"
ck_table = "online_retail"


@task
def extract():
    # Get data from postgres
    sql_fpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sql/{}.sql".format(sql))
    query = load_query(sql_fpath)
    raw_df = get_pandas_df(postgres_conn_id="postgres_db", db_name="postgres", query=query)

    if raw_df.empty:
        logger.info("Dataframe is empty")
        raise AirflowSkipException
    return raw_df


@task
def transform(raw_df):
    # Convert timezone
    raw_df['InvoiceDate'] = pd.to_datetime(raw_df['InvoiceDate']).dt.tz_convert('Asia/Ho_Chi_Minh')
    return raw_df


@task
def load(inserted_df):
    # Load data into clickhouse table
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_db')
    insert_query = 'INSERT INTO {} ({}) VALUES'.format(ck_table, ','.join(list(inserted_df.columns)))
    ch_hook.run(insert_query, inserted_df.to_dict('records'))


@dag(dag_id=dag_id,
     tags=[tag],
     schedule_interval='0 * * * *',
     start_date=datetime(year=2022, month=10, day=4),
     dagrun_timeout=timedelta(seconds=300),
     catchup=False,
     default_args=default_args)


def run():
    get_raw_data = extract()
    trans = transform(get_raw_data)
    load_to_ck = load(trans)
    get_raw_data >> trans >> load_to_ck

dag = run()

