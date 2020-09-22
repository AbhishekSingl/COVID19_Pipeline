# Description: In this script, we're aggregating emotions attributes based on "date" and "state".

import sys
import pandas as pd
import datetime
import os
from Utilities import load_config, list_files, retrieve_file, store_file, exists
import argparse

# Global Variables
PROCESSED_PATH = ''
WEEKLY_DATA_PATH = ''
DAILY_DATA_PATH = ''


def update_global_variables():
    global PROCESSED_PATH, WEEKLY_DATA_PATH, DAILY_DATA_PATH
    PROCESSED_PATH = os.getenv('PROCESSED_PATH')
    WEEKLY_DATA_PATH = os.getenv('WEEKLY_DATA_PATH')
    DAILY_DATA_PATH = os.getenv('DAILY_DATA_PATH')


def agg_at_daily_level(storage_type, incremental=True):
    """
    Aggregating at day level and storing in single or daily format based on given storage_type.
    :param str storage_type: ("single", "daily") this indicates whether we want to store a single file or
                                break it at day-level
    :param bool incremental: process all files or just the new files
    :return: None
    """

    filenames = list_files(WEEKLY_DATA_PATH, format='csv')
    current_year = str(datetime.date.today().year)

    if len(filenames) == 0:
        print('No file exists. Kindly add files in WEEKLY_DATA_PATH')
        sys.exit(0)

    if incremental and exists('{}/Last_Week_Processed.pkl'.format(PROCESSED_PATH)):
        file_list = []
        max_week_num = retrieve_file(PROCESSED_PATH, 'Last_Week_Processed.pkl')
        for filename in filenames:
            week_num = int(current_year + filename.split('_')[0][4:])
            if week_num >= max_week_num:
                file_list.append(filename)
        filenames = file_list
    else:
        max_week_num = int(current_year + '00')
    combined = []
    print("Processing following file(s):")
    for filename in filenames:
        print(filename)
        week_num = int(current_year + filename.split('_')[0][4:])
        if max_week_num < week_num:
            max_week_num = week_num
        df = retrieve_file(WEEKLY_DATA_PATH, filename, sep='\t')
        df = df[~df.duplicated()]
        df = df[df['country'].isin(['US', 'United States'])]
        req_cols = (['date', 'state', 'positive', 'trust', 'anger', 'fear', 'negative',
                    'sadness', 'anticipation', 'joy', 'surprise', 'disgust'])
        cols_list = list(set(df.columns).intersection(req_cols))
        df = df[~df['state'].isnull()][cols_list]
        grouped = df.groupby(['date', 'state'], as_index=False).mean().reset_index()
        combined.append(grouped)

    combined_df = pd.concat(combined, axis=0, sort=False)
    if storage_type == 'single':
        store_file(combined_df, DAILY_DATA_PATH, 'Day_Level_Agg.csv', sep='\t')
    elif storage_type == 'daily':
        for d in combined_df['date'].unique():
            date_record = combined_df[combined_df['date'] == d]
            store_file(date_record, DAILY_DATA_PATH, '{}_tweets.csv'.format(d), sep='\t')

    store_file(max_week_num, PROCESSED_PATH, 'Last_Week_Processed.pkl')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, help='Configuration File Path',
                        required=True)
    parser.add_argument('-i', '--incremental', type=eval, help='Incremental Status',
                        choices=[True, False], default=True)
    parser.add_argument('-s', '--storage', type=str.lower, help='Storage Type',
                        choices=["single", "daily"], required=True)
    args = parser.parse_args()
    load_config(args.config)
    update_global_variables()
    agg_at_daily_level(args.storage, args.incremental)
    print('DONE')
