import xml.etree.ElementTree as ET
import pandas as pd
from utils import (
    logger,
    add_columns,
    adjust_df_datatypes,
)
# from datetime import datetime, timedelta, timezone

def create_req_res_dataframe(data: list[dict]) -> pd.DataFrame:
    """ Create individual dataframes for the request and response type XML messages to upload to respective GCP tables. """

    df = pd.DataFrame(data)

    add_columns(df)
    adjust_df_datatypes(df)

    df = df.where(pd.notna(df), None)
    df = df.replace({'nan': None, 'NaT': None})

    request_df = df[df['TYPE'] == 'REQUEST']
    response_df = df[df['TYPE'] == 'RESPONSE']
    
    response_df = response_df.drop_duplicates()
    request_df = request_df.drop_duplicates()

    return request_df, response_df

def create_dataframes(df: pd.DataFrame) -> pd.DataFrame:
    """Create the XML tree, review the queried dataframe and determine if each row is RESPONSE or REQUEST. Based on that XML data is parsed and returned in another function. 
    Additional columns are added depending on the TYPE. Each row parsed as data is a new dataframe and is all merged together into a single dataframe using XACT_ID which is returned."""

    namespace: dict = {
        'ns': 'http://www.opentravel.org/OTA/2003/05'
    }

    result_dicts: list = []

    def process_row_wrapper(row, namespace):
        data: list = process_row(row, namespace)
        if data is not None:
            result_dicts.extend(data)

    df.apply(process_row_wrapper, namespace=namespace, axis=1)
    
    request_df, response_df = create_req_res_dataframe(result_dicts)
    combined_dataframe = request_df.set_index(['XACT_ID', 'MSG_ID']).combine_first(response_df.set_index(['XACT_ID', 'MSG_ID'])).reset_index()
 
    return combined_dataframe, request_df, response_df

def process_row(row: pd.Series, namespace: dict) -> list[dict]:
    try:
        tree = ET.fromstring(row['XML_DATA'])
    except Exception as _:
        logger.warning(f"{row['XACT_ID']}, Error: {row['XML_DATA']}")
        return None

    if row['TYPE'] == 'RESPONSE':
        data: list = parse_response(row, namespace, tree)
        if data:
            return data

    elif row['TYPE'] == 'REQUEST':
        data: list = parse_request(row, namespace, tree)
        if data:
            return data

def parse_response(row: pd.Series, namespace: dict, tree) -> list[dict]:
    availability_tag = tree.findall('<string>', namespaces=namespace)

    response_data: list = []

    if availability_tag is not None:
        for availability in availability_tag:
            tag = availability.attrib
            hotel_code = tag.get('HotelCode')

            time_span = availability.findall('ns:TimeSpan', namespaces=namespace)
            if time_span is not None:
                for time in time_span:
                    start_date: str = time.get('Start')
                    end_date: str = time.get('End')
                    change_mask: str = time.get('ChangeDateMask', None)

                    response_data.append({
                        'CREATED_TIMESTAMP': row["CREATED_TS"],
                        'XACT_ID': row["XACT_ID"].strip(), 
                        'MSG_ID': row['MSG_ID'].strip(),
                        'HOTEL_LIST': row['HOTEL_CODES'],
                        'HTL_CD': hotel_code, 
                        'START_DATE': start_date, 
                        'END_DATE': end_date, 
                        'CHANGE_DT_MASK': change_mask, 
                        'OPERATION_NAME': row['OPERATION_NAME'],
                        'VENDOR': row['VENDOR'],
                        'RESPONSE_CODE': row['RESPONSE_CODE'],
                        'TYPE': row['TYPE'],
                        })
    
    return response_data

def parse_request(row: pd.Series, namespace: dict, tree) -> list[dict]:
    stay_date_range = tree.findall('<string>', namespaces=namespace)                
    search_criteria = tree.findall('<string>', namespaces=namespace)

    stay_start_date = None
    stay_end_date = None
    change_ts = None
    availability = None
    rate = None
    modified = None

    if row['HOTEL_CODES'] != "[]":
        hotel_list: list = [hotel.strip() for hotel in row['HOTEL_CODES'][1:-1].replace('"', "").split(',')]
    else:
        hotel_list: list = []

    change_ts = tree.attrib.get('TimeStamp')

    if stay_date_range is not None:
        for stay_date in stay_date_range:
            stay_start_date: str = stay_date.get('Start')
            stay_end_date: str = stay_date.get('End')
    
    if search_criteria is not None:
        for search in search_criteria:
            availability: str = search.get('Availability')
            rate: str = search.get('Rate')
            modified: str = search.get('ModifiedSince')
    
    table_data = [{
            'XACT_ID': row['XACT_ID'].strip(),
            'MSG_ID': row['MSG_ID'].strip(),
            'CREATED_TIMESTAMP': row['CREATED_TS'],
            'VENDOR': row['VENDOR'],
            'STAY_DATE_START': stay_start_date,
            'STAY_DATE_END': stay_end_date,
            'OTA_TIMESTAMP': change_ts,
            'AVAIL_REQ': availability,
            'RATE_REQ': rate,
            'MODIFIED_SINCE': modified,
            'OPERATION_NAME': row['OPERATION_NAME'],
            'HOTEL_LIST': row['HOTEL_CODES'],
            'HOTEL_LIST_COUNT': len(hotel_list),
            'TYPE': row['TYPE'],
            }]
    
    return table_data
    