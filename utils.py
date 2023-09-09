import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, filename='info.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ Add new columns to exist Dataframe. """

    date_format = '%Y-%m-%d'

    try:
        df['TOTAL_CHANGES'] = df['CHANGE_DT_MASK'].apply(lambda x: x.count('1') if isinstance(x, str) else 0)
        df['TTL_DAYS_RES'] = df['CHANGE_DT_MASK'].apply(lambda x: len(x) if isinstance(x, str) else 0)
        df['MODIFIED_SINCE'] = pd.to_datetime(df['MODIFIED_SINCE'], errors='coerce')
        df['START_DATE_LEAD_TIME'] = df.apply(lambda row: int((row['CREATED_TIMESTAMP'] - row['MODIFIED_SINCE']).total_seconds() / 60)
        if not pd.isna(row['MODIFIED_SINCE']) and isinstance(row['MODIFIED_SINCE'], datetime)
        else None,axis=1)

        df['STAY_DATE_START'] = df['STAY_DATE_START'].astype(str)
        df['STAY_DATE_END'] = df['STAY_DATE_END'].astype(str)

        df['STAY_DATE_START'] = pd.to_datetime(df['STAY_DATE_START'].str.rstrip('Z'), format=date_format, errors='coerce')
        df['STAY_DATE_END'] = pd.to_datetime(df['STAY_DATE_END'].str.rstrip('Z'), format=date_format, errors='coerce')
        df['TTL_DAYS_REQ'] = ((df['STAY_DATE_END'] - df['STAY_DATE_START']).dt.days + 1)

    except Exception as e:
        print("Missing Column:", e)

    return df

def adjust_df_datatypes(df: pd.DataFrame) -> pd.DataFrame:
    """ Modify the data types in the DataFrame. This is preparation for uploading to GCP to avoid any datatype errors as well as performance improvements for the rest of the application process. """

    columns_to_clean: dict = {
        'XACT_ID': str,
        'MSG_ID': str,
        'HOTEL_LIST': str,
        'HTL_CD': str,
        'CHANGE_MASK': str,
        'OPERATION_NAME': str,
        'VENDOR': str,
        'RATE_REQ': str,
        'AVAIL_REQ': str,
        'TOTAL_CHANGES': pd.Int16Dtype(),
        'TTL_DAYS_RES': pd.Int16Dtype(),
        'START_DATE_LEAD_TIME': pd.Int32Dtype(),
        'TTL_DAYS_REQ': pd.Int16Dtype(),
        'HOTEL_LIST_COUNT': pd.Int16Dtype(),
        'RESPONSE_CODE': pd.Int16Dtype(),
        'OTA_TIMESTAMP': 'datetime64[ns, UTC]',
        'STAY_DATE_START': 'datetime64[ns, UTC]',
        'STAY_DATE_END': 'datetime64[ns, UTC]',
        'MODIFIED_SINCE': 'datetime64[ns, UTC]',
        'CREATED_TIMESTAMP': 'datetime64[ns, UTC]'
    }

    df['CREATED_TIMESTAMP'] = df['CREATED_TIMESTAMP'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    if 'MODIFIED_SINCE' in df.columns:
        for index, row in df.iterrows():
            if not pd.isna(row['MODIFIED_SINCE']):
                df.at[index, 'MODIFIED_SINCE'] = row['MODIFIED_SINCE'].strftime('%Y-%m-%d %H:%M:%S')
        

    for col_name, col_dtype in columns_to_clean.items():
        if col_name in df.columns:
            if col_dtype == str:
                df.loc[:, col_name] = df[col_name].astype(str)
            elif col_dtype == pd.Int64Dtype():
                df.loc[:, col_name] = df[col_name].astype(pd.Int64Dtype())
            elif 'datetime64' in str(col_dtype):
                date_format = '%Y-%m-%d %H:%M:%S'
                df.loc[:, col_name] = pd.to_datetime(df[col_name], errors='coerce', utc=True if 'UTC' in col_dtype else False, format=date_format)

    return df

if __name__ == '__main__':
    pass