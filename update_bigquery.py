from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
from utils import logger
from queries import delete_query

def write_to_request_table(df: pd.DataFrame, table_id: str):
    """ Defines what columns from the dataframe are request specific, cleans up the dataframe by explicitedly defining date formats, data types, and removing None, NaN, and NaT data then uploads to the appropriate table in GCP. """

    project_id: str = '<project-id>'
    dataset_id: str = '<dataset-id>'
    table_id: str = table_id

    sorted_df: pd.DataFrame = df[df['TYPE'] == 'REQUEST'][['XACT_ID', 'MSG_ID', 'CREATED_TIMESTAMP', 
                                            'VENDOR', 'OTA_TIMESTAMP', 'MODIFIED_SINCE', 'STAY_DATE_START', 'STAY_DATE_END', 
                                            'TTL_DAYS_REQ', 'AVAIL_REQ', 'RATE_REQ', 'START_DATE_LEAD_TIME', 'HOTEL_LIST', 'HOTEL_LIST_COUNT', 'OPERATION_NAME']]


    logger.info('Writing to Request Table')
    to_gbq(sorted_df, destination_table=f"{project_id}.{dataset_id}.{table_id}", if_exists='append')

def write_to_response_table(df: pd.DataFrame, table_id: str):
    project_id = '<project-id>'
    dataset_id = '<dataset-id>'
    table_id = table_id

    sorted_df = df[df['TYPE'] == 'RESPONSE'][['XACT_ID', 'MSG_ID', 'CREATED_TIMESTAMP', 'VENDOR', 'HTL_CD', 'START_DATE', 'END_DATE', 'TTL_DAYS_RES', 'CHANGE_DT_MASK', 'TOTAL_CHANGES', 'OPERATION_NAME']]

    logger.info('Writing to Response Table')
    to_gbq(sorted_df, destination_table=f"{project_id}.{dataset_id}.{table_id}", if_exists='append')

def write_to_summary_table(df: pd.DataFrame, table_id: str):

    project_id = '<project-id>'
    dataset_id = '<dataset-id>'
    table_id = table_id

    df = df.drop('TYPE', axis=1)

    logger.info('Writing to Summary Table')
    to_gbq(df, destination_table=f"{project_id}.{dataset_id}.{table_id}", if_exists='append')

def delete_duplicates_from_table(date: str, current_hour: int, table_id: str):

    project_id = 'ca-sbox-owner-fran-anly-444'

    client = bigquery.Client(project=project_id)

    query = delete_query(table_id, date, current_hour)

    query_job = client.query(query)
    query_job.result()
    
    rows_deleted = query_job.num_dml_affected_rows
    if rows_deleted > 0:
        logger.info(f"Number of rows deleted: {rows_deleted} from {table_id}")