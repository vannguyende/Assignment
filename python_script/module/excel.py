
import os
import pandas as pd
import logging
logger = logging.getLogger('excel')


def is_exist_file(file_path):
    """
    used to check if a particular regular file exists or not
    :param file_path: path to file
    :return: Bool
    """
    return os.path.exists(file_path)


def read_exec_file(file_path, sheet_name, num_rows=None):
    """
    Read Excel file from local machine with specific number of row
    :param file_path: path to file
    :param sheet_name: Excel worksheet name
    :param num_rows: number of first n rows
    :return: dataframe
    """
    df = pd.DataFrame()

    if is_exist_file(file_path):
        xls = pd.ExcelFile(file_path)
        df = pd.read_excel(xls, sheet_name)
        if num_rows and num_rows < len(df):
            df = df.iloc[:num_rows]
    else:
        logger.info('File Not around')  
    return df
