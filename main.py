import pandas as pd
import os
from utils import (
    logger,)
from update_bigquery import (
    write_to_request_table,
    write_to_response_table,
    write_to_summary_table,
    delete_duplicates_from_table,
    )
from parse_data import (
    create_dataframes,
)
from queries import (
    query_msg_table,
    query_backfill_records
)

os.environ['GOOGLE_CLOUD_PROJECT'] = '<project_id>'

def process_backfill_changes(data: dict):
    dataframe = data['df']

    try:
        summary_df, request_df, response_df = create_dataframes(dataframe)

        if not summary_df.empty:
            logger.info(f'Count of combined DF for Backfill: {len(summary_df)}')

            write_to_request_table(request_df, 'CDS_REQUEST')
            write_to_response_table(response_df, 'CDS_RESPONSE')

            write_to_summary_table(summary_df, 'CDS_SUMMARY')
        
        logger.debug('No backfill data to process.')
    except Exception as e:
        logger.info('Error: %s', e)

def process_esb_msg_data(entry: pd.DataFrame):
    """ Creates dataframes for the parsed xml messages and then sends each dataframe to delete data and write data to the correct GCP tables. """

    dataframe: pd.DataFrame = entry['df']
    date: str = entry['date']
    current_hour: int = entry['hour']

    try:
        summary_df, request_df, response_df = create_dataframes(dataframe)

        delete_duplicates_from_table(date, current_hour, 'CDS_REQUEST')
        write_to_request_table(request_df, 'CDS_REQUEST')

        delete_duplicates_from_table(date, current_hour, 'CDS_RESPONSE')
        write_to_response_table(response_df, 'CDS_RESPONSE')

        delete_duplicates_from_table(date, current_hour, 'CDS_SUMMARY')  
        write_to_summary_table(summary_df, 'CDS_SUMMARY')

    except Exception as e:
        logger.info('Error: %s', e)

def run_manual_process(date, current_hour):
    """ Optional functionality only utilized when manual_run.py is the file ran. """
    data_dict = []

    query_df, date, current_hour = query_msg_table(date=date, current_hour=current_hour, manual_run=True)

    data = {
        'df': query_df,
        'date': date,
        'hour': current_hour,
        'backfill_indicator': False,
    }    
    data_dict.append(data)

    for entry in data_dict:
        if not entry['df'].empty:
            if entry['backfill_indicator']:
                logger.info('Processing Backfill')
                process_backfill_changes(entry)
            else:
                logger.info('Processing ESB_MSG')
                process_esb_msg_data(entry)
        else:
            logger.info('Empty Dataframe, skipped')

if __name__ == "__main__":

    data_dict = []

    query_df, date, current_hour = query_msg_table()

    data = {
        'df': query_df,
        'date': date,
        'hour': current_hour,
        'backfill_indicator': False,
    }    
    data_dict.append(data)

    backfill_df = query_backfill_records(current_hour)
    backfill_data = {
        'df': backfill_df,
        'backfill_indicator': True, 
    }    
    data_dict.append(backfill_data)

    for entry in data_dict:
        if not entry['df'].empty:
            if entry['backfill_indicator']:
                logger.info('Processing Backfill')
                process_backfill_changes(entry)
            else:
                logger.info('Processing ESB_MSG')
                process_esb_msg_data(entry)
        elif entry['df'].empty and entry['backfill_indicator']:
            logger.info('Backfill Dataframe was empty')
        else:
            logger.info('Empty Dataframe, skipped')