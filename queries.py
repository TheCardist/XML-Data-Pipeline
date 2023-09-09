from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
from typing import Tuple
from utils import (
    logger,
)

def query_backfill_records(current_hour: int) -> pd.DataFrame:
    """ Query the master table and the summary table to compare for select dates as to what records are missing from the summary table. Whatever is missing is then pulled for processing. """
    
    project_id: str = '<project_id>'

    client = bigquery.Client(project=project_id)

    # Optional arguments [limit, three_days_prior, two_days_prior]
    query: str = get_backfill_query(current_hour, limit=10000)

    query_job = client.query(query)

    df: pd.DataFrame = query_job.to_dataframe()

    return df

def query_msg_table(**kwargs) -> pd.DataFrame:
    """Query the master table to get the records that are between two timestamps and return dataframe and the end_timestamp which is stored to use as the 'previous timestamp' on the next run."""

    project_id: str = '<project_id>'

    # Optional keyword arguments [limit, date, current_hour, manual_run]
    query, date, current_hour = get_esb_msg_query(**kwargs)
    
    client = bigquery.Client(project=project_id)
    
    query_job = client.query(query)

    df: pd.DataFrame = query_job.to_dataframe()
    logger.info(f'ESB_MSG query for: {date} at hour: {current_hour}')

    return df, date, current_hour

def get_esb_msg_query(**kwargs) -> Tuple[str, str, int]:
    """ Return the query for the ESB_MSG table because on prior date and current hour. """

    date_format: str = '%Y-%m-%d'

    # Setup for manual runs where the dates and timestamps can we specified.
    if kwargs.get('manual_run', False) == True:
        date: str = kwargs.get('date')
        current_hour: int = kwargs.get('current_hour')
    else:
        # Date is 1 day prior to today
        date: str = (datetime.now() - timedelta(days=1)).strftime(date_format)
        current_hour: int = datetime.now().hour


    esb_msg: str = f"""
    SELECT *
    FROM `<table>`
    WHERE datepart = '{date}'
        AND (operationName = '<value>' or operationName = '<value2>')
        AND extract(hour from PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3S%Ez', createdTimestamp)) = {current_hour}
    """
    
    if 'limit' in kwargs:
        esb_msg += f"LIMIT {kwargs.get('limit')}"

    return esb_msg, date, current_hour

def get_backfill_query(current_hour: int, **kwargs) -> str:
    """ Get the query for the backfill process based on 7 days prior and current hour. """

    date_format: str = '%Y-%m-%d'

    date = (datetime.now() - timedelta(days=7)).strftime(date_format)

    # Calculate end out which may roll over to another day.
    end_hour: int = (current_hour + 1) % 24

    end_date: str = date
    if current_hour == 23:
        end_date: str = (datetime.strptime(date, date_format) + timedelta(days=1)).strftime(date_format)
    
    start_timestamp: str = f'{date} {current_hour:02d}:00:00'
    end_timestamp: str = f'{end_date} {end_hour:02d}:00:00'

    backfill_query: str = f"""
    SELECT *
    FROM `<table>`
    WHERE datepart = '{date}'
        AND (operationName = '<value>' or operationName = '<value2>')
        AND extract(hour from PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3S%Ez', createdTimestamp)) = {current_hour}
        AND concat(transactionid, '-', messageid) not in (
            SELECT concat(xact_id, '-', msg_id) 
            FROM `<table>`
            where created_timestamp between '{start_timestamp}' and '{end_timestamp}'
            )
    """
    if 'limit' in kwargs:
        backfill_query += f" LIMIT {kwargs['limit']}"

    logger.info(f'Backfill for {date}')
    return backfill_query

def delete_query(table_id: str, date: str, current_hour: int) -> str:

    query = f"""DELETE FROM `<project/dataset>.{table_id}` 
    WHERE EXTRACT(HOUR FROM CREATED_TIMESTAMP) = {current_hour}
    AND DATE(CREATED_TIMESTAMP) = '{date}'"""

    return query

if __name__ == "__main__":
    print(get_esb_msg_query())