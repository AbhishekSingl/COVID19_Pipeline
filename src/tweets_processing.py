import copy
import os
import pandas as pd
import argparse
from collections import defaultdict
import re
import datetime
from Utilities import load_config, store_file, retrieve_file, list_files, exists
os.chdir(os.getcwd())

# Global Variables
PATH = ''
PROCESSED_PATH = ''
WEEKLY_DATA_PATH = ''
FLAT_FILES_PATH = ''
CURRENT_YEAR = datetime.date.today().year


def update_global_variables():
    global PATH, PROCESSED_PATH, WEEKLY_DATA_PATH, FLAT_FILES_PATH
    PATH = os.getenv('PATH')
    PROCESSED_PATH = os.getenv('PROCESSED_PATH')
    WEEKLY_DATA_PATH = os.getenv('WEEKLY_DATA_PATH')
    FLAT_FILES_PATH = os.getenv('FLAT_FILES_PATH')


def calendar_generation(year):
    """
    Generating calendar for the given year.

    :param year: year for which calendar needs to generate
    :return: A dataframe with 365 rows with week, month, quarter information for each date
    """
    print('Generating Calendar for {} year.'.format(year))
    start_date = '1/1/' + str(year)
    end_date = '12/31/' + str(year)
    dates_df = pd.DataFrame(pd.date_range(start=start_date, end=end_date), columns=['date'])
    dates_df['week'] = dates_df['date'].dt.week
    dates_df['month'] = dates_df['date'].dt.month
    dates_df['year'] = dates_df['date'].dt.year
    dates_df['quarter'] = dates_df['date'].dt.quarter
    dates_df['day'] = dates_df['date'].dt.weekday
    store_file(dates_df, PROCESSED_PATH, "calendar_{}.csv".format(year))
    return dates_df


def week_min_max_date_dict(dates_df, year):
    """
    Here, we're determining START and END date for each week.

    :param dates_df: calendar of a given year
    :param year: current year
    :return: a nested dictionary with week number as primary key followed
    by max_date and min_date corresponding to that week.
    """
    grouped = dates_df.groupby(['week']).agg({'date': [max, min]})
    grouped.columns = ['max_date', 'min_date']
    grouped.reset_index(inplace=True)
    grouped['max_date'] = grouped['max_date'].dt.date
    grouped['min_date'] = grouped['min_date'].dt.date
    calendar_dict = defaultdict(lambda: dict())
    for i in range(len(grouped)):
        week = grouped['week'].iloc[i]
        calendar_dict[week] = {'max_date': grouped['max_date'].iloc[i], 'min_date': grouped['min_date'].iloc[i]}

    calendar_dict = dict(calendar_dict)

    store_file(calendar_dict, PROCESSED_PATH, "week_min_max_dict_{}.pkl".format(year))

    return calendar_dict


def data_preprocessing(filepath, filename, req_col):
    """
    We're doing following tasks in the function:
    1. Keeping tweets with "English" language"
    2. Extracting hashtags from each tweets instead of using the existing hashtag field
    given by Twitter API because that field is not come with all hashtags present in "text" column
    3. Creating a "Cleaned Text" column after removing all special characters and spaces.
    4. Emotion classification using NRC classifier.

    :param filepath: file path of raw data files.
    :param filename: file name of each raw file.
    :param req_col: passing a list of important columns
    :return: returning a processed data frame.
    """
    data = retrieve_file(filepath, filename, usecols=req_col)
    # Removing records with duplicate tweet id
    data = data[~data.status_id.duplicated()].reset_index(drop=True)
    # Considering only english language tweets
    data = data[data['lang'] == 'en']
    data.reset_index(inplace=True, drop=True)
    # Removing the \r, \t characters which the texts into new rows while importing into BI tools.
    data['text'] = data.text.apply(lambda x: re.sub("(\\r+)|(\r+)|(\t+)|(\\t+)", "", re.sub("\s\s+", " ", x.lower())))
    # Creating hashtags
    data['hashtags'] = data.text.apply(
        lambda x: str(re.findall('[#]\w+', x)).replace('[', '').replace(']', '').replace(', ', ';'))
    # Cleaning Text
    data['cleaned_text'] = data.text.apply(
        lambda x: re.sub("(<u\+\S*>)|([#]\w+)|(\w+:\/\/\S+)|([^0-9A-Za-z ])|(\s\s+)", "", x))
    # Creating date
    data['date'] = pd.to_datetime(data['created_at'], yearfirst=True).dt.date
    # Creating week
    data['week'] = pd.to_datetime(data['date']).dt.week
    # Emotional features
    emotion_info = get_emotions(data.cleaned_text)
    # Combining emotion_info to dataframe
    data = pd.concat([data, emotion_info], axis=1)

    return data


def main(storage, locations, region, incremental=True):
    """
    Data cleaning, emotion classification and extracting geo information from multiple columns by
    assigning value into "city", "state", "county" and "country".

    :param storage: this defines whether you want to store processed data in a "Single" file
    or break into "Weekly" files i.e. 4 files every month.
    :param locations: list of locations for which you fetched the data using Twitter API. Make sure location details
    exactly matches with your search queries
    :param region: country name
    :param incremental: Bool value defines whether to process the data for new files or for all files.
    :return: return processed data with emotions and geo information
    """
    req_col = ['user_id', 'screen_name', 'status_id', 'created_at',
               'text', 'source', 'is_retweet', 'retweet_count', 'hashtags', 'status_url',
               'urls_t.co', 'lang', 'retweet_created_at', 'verified', 'retweet_location', 'location', 'bbox_coords']
    all_tables = []
    for loc in locations:
        tables = []
        filepath = f"{PATH}/{loc}"
        filenames = list_files(filepath)
        # Incremental loading
        if incremental and exists(f"{PROCESSED_PATH}/files_{loc}.pkl"):
            # Checking if any new file exist. If so, process only that file instead of processing all files again.
            old_filenames = retrieve_file(PROCESSED_PATH, "files_{}.pkl".format(loc))
            org_filenames = copy.copy(filenames)
            filenames = list(set(filenames) - set(old_filenames))
            if len(filenames) > 0:
                store_file(org_filenames, PROCESSED_PATH, "files_{}.pkl".format(loc))
                del org_filenames
            else:
                print('No new file is present.')
                return
        else:
            store_file(filenames, PROCESSED_PATH, "files_{}.pkl".format(loc))

        print("--Processing following file(s) for location: {}".format(loc))
        for file in filenames:
            if ('last_tweet' not in file) and ('first_tweet' not in file):
                print(file)
                tables.append(data_preprocessing(filepath, file, req_col))
        data = pd.concat(tables, axis=0, sort=False)

        data = data[~data.status_id.duplicated()].reset_index(drop=True)

        # Geotagging
        data = geo_tagging(data, region)

        # Changing the order of "Text" column and keeping it as the last column
        text_info = data['text']
        data.drop('text', axis=1, inplace=True)
        data['text'] = text_info
        del text_info

        # Combining data for available locations.
        all_tables.append(data)

    print('--Storing')
    data = pd.concat(all_tables, axis=0, sort=False)

    if 'single' in storage:
        file_exist = (not exists(f'{PROCESSED_PATH}/Processed_Tweets.csv')) or (not incremental)
        store_file(data, PROCESSED_PATH, 'Processed_Tweets.csv', sep='\t', mode='a', header=file_exist)

    if 'weekly' in storage:
        if exists(f'{PROCESSED_PATH}/week_min_max_dict_{CURRENT_YEAR}.pkl'):
            calendar = retrieve_file(PROCESSED_PATH, 'week_min_max_dict_{}.pkl'.format(CURRENT_YEAR))
        else:
            dates = calendar_generation(CURRENT_YEAR)
            calendar = week_min_max_date_dict(dates, CURRENT_YEAR)

        # Dividing data into weeks.
        for w in data['week'].unique():
            weekly_df = data[data['week'] == w]

            filename = 'Week{}_{}_{}.csv'.format(w, calendar[w]['min_date'], calendar[w]['max_date'])
            file = '{}/{}'.format(WEEKLY_DATA_PATH, filename)

            file_exist = (not exists(file)) or (not incremental)
            store_file(weekly_df, WEEKLY_DATA_PATH, filename, '\t', mode='a', header=file_exist)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--storage', type=str.lower, help='Storage type',
                        choices=["single", "weekly"], required=True)
    parser.add_argument('-i', '--incremental', type=eval, help='Incremental Status',
                        choices=[True, False], default=True)
    parser.add_argument('-c', '--config', type=str.lower, help='Configuration Path',
                        required=True)
    parser.add_argument('-l', '--locations', type=str.lower, default="usa",
                        help='semi-colon(;) separated list of locations. For example: "California, '
                             'USA;Texas, Austin, USA"')
    parser.add_argument('-r', '--region', type=str.lower, help='country name',
                        default="usa")
    args = parser.parse_args()
    # Creating folder names where data is stored.

    locations_list = []

    import re
    for m in args.locations.split(';'):
        locations_list.append(re.sub("[^0-9_a-zA-Z]+", "", re.sub('\s+', '_', m.strip())))

    load_config(args.config)
    update_global_variables()

    # Loading Libraries (this has to be load after "load_config" function)
    from emotions_info import get_emotions
    from locations_info import geo_tagging

    main(args.storage, locations_list, args.region, args.incremental)
    print("--Processing Done")
