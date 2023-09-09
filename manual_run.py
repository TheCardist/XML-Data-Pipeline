from main import run_manual_process
from datetime import datetime, timedelta
import time

if __name__ == "__main__":
    """ This function was created to run the entire script for custom days incase of missing data for extended periods that needed to be captured. """
    
    dt_format: str = '%Y-%m-%d'

    # Enter start, stop, and current how to begin the manual run process
    start_date: str = '2023-09-06'
    stop_date: str = '2023-09-08'
    current_hour: int = 22

    previous_datetime: datetime = datetime.strptime(start_date, dt_format)
    stop_datetime: datetime = datetime.strptime(stop_date, dt_format)

    while previous_datetime != stop_datetime:

        run_manual_process(start_date, current_hour)

        current_hour: int = (current_hour + 1) % 24
        if current_hour == 0:
            previous_datetime: datetime = previous_datetime + timedelta(days=1)
            start_date: str = previous_datetime.strftime(dt_format)