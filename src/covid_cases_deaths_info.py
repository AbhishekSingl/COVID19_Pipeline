import io

import pandas as pd
import numpy as np
import requests
import os
from Utilities import load_config, store_file, retrieve_file

PROCESSED_PATH = ''
WEEKLY_DATA_PATH = ''
FLAT_FILES_PATH = ''


def population_info():
    # Population of each state in USA
    pop = retrieve_file(FLAT_FILES_PATH, "PopulationEstimates.xls", skiprows=2,
                        usecols=['State', 'POP_ESTIMATE_2019'])
    pop = pop[pop['State'] != 'US']
    pop = pd.DataFrame(pop.groupby('State').apply(lambda x: x['POP_ESTIMATE_2019'].iloc[0]),
                       columns=['Population']).reset_index()
    pop['Population'] = pop['Population'].astype('int')

    # Population of each USA Commonwealth and Territories

    territories = retrieve_file(FLAT_FILES_PATH, 'US_Territories_Pop.csv')
    territories['code'] = ['PR', 'GU', 'VI', 'MP', 'AS']
    territories['pop2020'] = (territories['pop2020'] * 1000).astype('int')
    territories = territories[['code', 'pop2020']]
    territories.columns = pop.columns
    territories = territories[~territories['State'].isin(pop['State'])]

    # Merging State and Territory Population Data
    pop = pd.concat([pop, territories])
    pop = dict(zip(pop['State'], pop['Population']))
    return pop


def covid_api():
    # Population Info
    pop = population_info()
    # Covid Cases, Deaths and Recovered info for all states day-wise
    req = requests.get('https://api.covidtracking.com/v1/states/daily.csv')
    url_content = req.content
    api_df = pd.read_csv(io.StringIO(url_content.decode('utf-8')))
    store_file(api_df, FLAT_FILES_PATH, 'CovidCasesDeaths.csv')
    del api_df
    covid_cases = retrieve_file(FLAT_FILES_PATH, 'CovidCasesDeaths.csv',
                                usecols=['date', 'state', 'positiveIncrease', 'negativeIncrease',
                                         'hospitalizedIncrease', 'recovered', 'deathIncrease'])
    covid_cases['date'] = pd.to_datetime(covid_cases['date'], format='%Y%m%d')

    # Normalizing Covid Cases, deaths and other features w.r.t population data
    col = ['positiveIncrease', 'negativeIncrease', 'hospitalizedIncrease', 'recovered', 'deathIncrease']
    covid_info = covid_cases.groupby(['date', 'state']).apply(lambda x: (x[col]*1000000 // pop[x['state'].iloc[0]]))
    covid_info.columns = np.array(covid_info.columns) + '_per_1M'
    covid_info = pd.concat([covid_cases[['date', 'state']], covid_info], axis=1)
    # Combining normalized and original columns
    covid_info = pd.concat([covid_info, covid_cases.drop(['date', 'state'], axis=1)], axis=1)

    # Storing
    store_file(covid_info, PROCESSED_PATH, 'CovidCasesDeaths_Processed.csv')


def update_global_variables():
    global PROCESSED_PATH, WEEKLY_DATA_PATH, FLAT_FILES_PATH
    PROCESSED_PATH = os.getenv('PROCESSED_PATH')
    WEEKLY_DATA_PATH = os.getenv('WEEKLY_DATA_PATH')
    FLAT_FILES_PATH = os.getenv('FLAT_FILES_PATH')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str.lower, help='Configuration Path',
                        required=True)
    args = parser.parse_args()
    load_config(args.config)
    update_global_variables()
    covid_api()
