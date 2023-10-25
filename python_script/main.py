import pandas as pd 
import const
from module.excel import read_exec_file
from module.postgres import Postgresql
from dotenv import load_dotenv
import os 
import argparse

env_path = "./db_info.env"
load_dotenv(dotenv_path=env_path)


def process_data(df):
    """
    """
    # Convert columns type
    int_cols = [const.CUSTOMER_ID, const.QUANTITY]
    str_cols = [const.INVOICE, const.STOCK_CODE, const.COUNTRY, const.DESCRIPTION]
    df[str_cols] = df[str_cols].astype(str)
    df[int_cols] = df[int_cols].apply(pd.to_numeric, errors='coerce').fillna(0, downcast='int')
    df[const.PRICE] = pd.to_numeric(df[const.PRICE], errors='coerce').fillna(0, downcast='float')
    df[const.INVOICE_DATE] = pd.to_datetime(df[const.INVOICE_DATE], errors='coerce').dt.tz_localize(tz='Asia/Ho_Chi_Minh')

    # Remove redundant charater in string columns
    df[str_cols] = df[str_cols].apply(lambda x: x.replace({' +':' ', '"':''}, regex=True).str.strip())
    df[int_cols] = df[int_cols].where(df[int_cols] > 0, 0)

    # Calculate the total of number of duplicate product for each transaction
    agg_func = {
        const.DESCRIPTION: "first",
        const.QUANTITY: "sum", 
        const.INVOICE_DATE: "max", 
        const.CUSTOMER_ID: "first", 
        const.PRICE: "first", 
        const.COUNTRY: "first"
    }
    df = df.groupby([const.INVOICE, const.STOCK_CODE], sort=False, as_index=False).agg(agg_func)
   
    # Rename columns name
    cols_map = {
        const.CUSTOMER_ID: const.CUSTOMER_ID.replace(" ", ""),
        const.INVOICE: "{}ID".format(const.INVOICE)
    }
    df = df.rename(columns=cols_map)
    return df


def load_to_pg(df, table):
    connection_info = {
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
        "database": os.getenv("PG_NAME"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD")
    }
    postgres = Postgresql(connection_info)
    insert = postgres.execute_batch(df=df, table=table)


def main():
    raw_data_file_path = "dataset/raw/online_retail_II.xlsx"
    output_file_path = "dataset/output/data_cleaned.pkl"
    sheet_name = 'Year 2009-2010'
    pg_table_name = "online_retail"

    df_raw = read_exec_file(raw_data_file_path, sheet_name, num_rows=1000)
    df_processed = process_data(df_raw)
    df_processed.to_pickle(output_file_path)

    # Load data to postgres db
    parser = argparse.ArgumentParser()
    parser.add_argument('--insert', action='store_true', help='the flag that insert data to postgres')
    args = parser.parse_args()

    if args.insert:
        load_to_pg(df=df_processed, table=pg_table_name)
    else:
        print("Run with debug mode without insert data to db")


if __name__ == "__main__":
    main()
