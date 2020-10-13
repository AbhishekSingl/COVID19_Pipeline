# Location Data
import json
import re
import sys
import time
import pandas as pd
import requests
import numpy as np
from Utilities import store_file, retrieve_file, exists
import os
import warnings
warnings.simplefilter('ignore')

FLAT_FILES_PATH = os.getenv('FLAT_FILES_PATH')
PROCESSED_PATH = os.getenv('PROCESSED_PATH')


def city_state_county():
    # Cities Information
    cities = retrieve_file(FLAT_FILES_PATH, "uscities.csv",
                           usecols=['city', 'state_id', 'state_name', 'county_name'])

    for i in list(cities):
        cities[i] = cities[i].str.lower()

    city_dict = {}
    for i in cities['state_id'].unique():
        city_dict[i] = list(cities[cities['state_id'] == i]['city'].unique())

    # County and State Information
    county_state_info = retrieve_file(FLAT_FILES_PATH, "PopulationEstimates.xls", skiprows=2,
                                      usecols=['State', 'Area_Name'])
    county_state_info = county_state_info[county_state_info['State'] != 'US']

    # State Info
    state_info = pd.DataFrame(county_state_info.groupby('State').apply(lambda x: x['Area_Name'].iloc[0]),
                              columns=['Name']).reset_index()
    state_dict = {}
    for i in range(state_info.shape[0]):
        state_dict[state_info.iloc[i]['State'].lower()] = state_info.iloc[i]['Name'].lower()
        state_dict[state_info.iloc[i]['Name'].lower()] = state_info.iloc[i]['State'].lower()

    # County Information
    county_info = county_state_info.groupby('State').apply(lambda x: x['Area_Name'].iloc[1:]).droplevel(1).reset_index()
    county_info['State'] = county_info['State'].str.strip().str.lower()
    county_info['Area_Name'] = county_info['Area_Name'].str.strip().str.lower()
    county_info['transformed_county_name'] = county_info['Area_Name'].str.replace('county', '').str.strip()

    county_dict = {}
    county_transformed_dict = {}
    for i in county_info['State'].unique():
        county_dict[i] = list(county_info[county_info['State'] == i]['Area_Name'])
        county_transformed_dict[i] = list(map(lambda x: x.lower(), county_info[county_info['State'] == i]['Area_Name']))

    return cities, city_dict, state_dict, county_dict, county_transformed_dict


def coord(df, flag='None', col_name='None'):
    """
    Here we're using Google API for reverse geocoding so basically we're passing LAT and LONG and getting
    corresponding address information.
    :param df: a dataframe with location information
    :param flag: it defines the flag where coordinates are present.
    :param col_name: it defines the corresponding column based on location type
    :return: dataframe with city, county, state, country information
    """
    coord_df = df[df['Flag_Loc'] == flag]
    coord_df = coord_df[[col_name]]
    coord_df = coord_df[~coord_df.duplicated()]
    coord_df = np.array(coord_df[col_name].str.split(' '))

    # Dictionary for caching the address information for each coordinate. This will help us to use the existing
    # address info instead of calling google api for every coordinate.
    if exists(f"{PROCESSED_PATH}/address_caching.pkl"):
        caching = retrieve_file(PROCESSED_PATH, "address_caching.pkl")
    else:
        caching = {}

    # Same information as above but we're using LIST here instead of dictionary. So, it's easy to convert it to
    # dataframe and then can be converted to csv or xlsx or any format.
    lat_lng = []

    # Reverse GeoCoding using Google Maps API
    for coords in coord_df:
        lng = str(np.mean(list(map(float, coords[:4]))))
        lat = str(np.mean(list(map(float, coords[4:]))))

        d = {}

        if caching.get(lat + '_' + lng, 0) != 0:
            d = caching[lat + '_' + lng]
        else:
            try:
                time.sleep(1)
                key = os.getenv('GOOGLE_API_KEY')
                URL = 'https://maps.googleapis.com/maps/api/geocode/json?latlng=' + lat + ',' + lng + '&key=' + key

                response = requests.request('GET', URL)
                address_json = json.loads(response.text)['results'][0]['address_components']

                for j in address_json:
                    d['lat'] = lat
                    d['lng'] = lng
                    types = "_".join(j['types'])
                    d[types + '_long'] = j['long_name']
                    d[types + '_short'] = j['short_name']

                caching[lat + '_' + lng] = d

                store_file(caching, PROCESSED_PATH, "address_caching.pkl")
            except IndexError:
                pass
            except Exception as e:
                print(e)
                sys.exit(0)

        lat_lng.append(d)

    address_info = pd.DataFrame(lat_lng)

    address_info = address_info[['lat', 'lng', 'locality_political_long',
                                 'administrative_area_level_2_political_long',
                                 'administrative_area_level_1_political_short',
                                 'country_political_long']]
    address_info.columns = ['lat', 'lng', 'city', 'county', 'state', 'country']

    df = df[df['Flag_Loc'] == flag]

    df['lng'] = df[col_name].str.split(' ').apply(lambda x: np.mean(list(map(float, x[:4])))).astype(str)
    df['lat'] = df[col_name].str.split(' ').apply(lambda x: np.mean(list(map(float, x[4:])))).astype(str)

    df = df.merge(address_info, how='left', left_on=['lat', 'lng'], right_on=['lat', 'lng'])
    df['flag'] = 'Success'
    return df


def retweetLoc(df, flag='None', col_name='None'):
    """
    :param df: a dataframe with location information
    :param flag: it defines the location type either based on "retweet" location or "user-specified" location.
    :param col_name: it defines the corresponding column based on location type
    :return: dataframe with city, county, state, country information
    """
    cities, city_dict, state_dict, county_dict, county_transformed_dict = city_state_county()
    df = df[df['Flag_Loc'] == flag]
    RTweetLoc = pd.DataFrame(df[col_name].unique(), columns=['Location'])
    RTweetLoc['Separated'] = RTweetLoc['Location'].str.split(',')
    RTweetLoc['len'] = RTweetLoc['Separated'].apply(lambda x: len(x))

    d = dict()
    RTweet_List = []
    for itr in range(RTweetLoc.shape[0]):

        for k in ['city', 'county', 'state', 'country', 'flag']:
            d[k] = ''
        try:
            for loc in RTweetLoc['Separated'].iloc[itr][::-1]:

                loc = re.sub("[^0-9A-Za-z ]", '', loc.strip().lower())
                loc = loc.replace('saint', 'st.')
                if (d['country'] == '') and (
                        loc in ['usa', 'united states', 'us', 'united states of america', 'america']):
                    d['country'] = 'US'
                    d['flag'] = 'Success'
                elif (d['state'] == '') and (loc in state_dict.keys()):
                    d['state'] = (((len(loc) > 2) and state_dict[loc]) or loc)
                    d['country'] = 'US'
                    d['flag'] = 'Success'
                elif (d['county'] == '') and (d['state'] != '') and (
                        (loc in county_dict[d['state']]) or (loc in county_transformed_dict[d['state']])):
                    d['county'] = loc
                    d['flag'] = 'Success'
                elif (d['state'] != '') and (loc in city_dict[d['state']]):
                    d['city'] = loc
                    d['country'] = 'US'
                    d['county'] = \
                        cities[(cities['city'] == loc) * (cities['state_id'] == d['state'])]['county_name'].iloc[0]
                    d['flag'] = 'Success'
                elif ((RTweetLoc['len'].iloc[itr] == 1) or (d['country'] == 'US')) and (loc in list(cities['city'])):
                    d['city'] = loc
                    d['country'] = 'US'
                    d['flag'] = 'Success'
                    d['state'] = (cities[cities['city'] == loc]['state_id'].iloc[0])
                    d['county'] = (cities[cities['city'] == loc]['county_name'].iloc[0])
                elif d['flag'] != 'Success':
                    d['flag'] = 'Conditions Not Met'
        except Exception as e:
            print(e, "\nContinuing.")
            d['flag'] = 'Exception'

        d['state'] = d['state'].upper()

        for k in ['city', 'county']:
            d[k] = d[k].capitalize()

        RTweet_List.append(d.copy())

    RTweetLoc.drop(['Separated', 'len'], axis=1, inplace=True)
    RTweetInfo = pd.concat([RTweetLoc, pd.DataFrame(RTweet_List)], axis=1)
    df['lng'] = 'NA'
    df['lat'] = 'NA'
    df = df.merge(RTweetInfo, how='left', right_on='Location', left_on=col_name)
    df.drop('Location', axis=1, inplace=True)

    return df


def geo_tagging(df, region):
    """
    Here, we're extracting city, county, state, country information based on following logic:
    Firstly, we're using data points where coordinates are present.
    Secondly, we're using "Retweet location" as it gives the latest location where a user was present
    at the time of tweeting.
    lastly, user's location given in his/her profile. This field is less reliable as nobody generally update their
    location and also it's a free string field i.e we can input any textual data without any particular format.

    :param df: dataframe with tweets information
    :param region: country name
    :return: dataframe with city, county, state, country information
    """

    if region.strip() != 'usa':
        print('Currently this script is applicable for USA only.\n'
              'Kindly update "location_info.py" for more countries.')
        sys.exit(0)

    # Filtering only english language
    df = df[df['lang'] == 'en']

    # Changing dta type of all required columns into string.
    df[['retweet_location', 'bbox_coords', 'location']] = df[['retweet_location', 'bbox_coords', 'location']].astype(
        'str')

    df['Flag_GeoCoord'] = (df['bbox_coords'] != 'NA NA NA NA NA NA NA NA').astype('int')
    df['Flag_ReTweetLoc'] = (df['retweet_location'] != 'nan').astype('int')
    df['Flag_UserProfileLoc'] = (df['location'] != 'nan').astype('int')

    df['Flag_Loc'] = df.apply(lambda x:
                              'GeoCoord' if x['Flag_GeoCoord'] else
                              ('ReTweetLoc' if x['Flag_ReTweetLoc'] else
                               ('UserProfileLoc' if x['Flag_UserProfileLoc'] else 'NA')), axis=1)

    df.drop(['Flag_GeoCoord', 'Flag_ReTweetLoc', 'Flag_UserProfileLoc'], axis=1, inplace=True)

    # Calling Functions to extract the city, county, state and Country information
    final_df = []

    if sum(df['Flag_Loc'] == 'GeoCoord') > 0:
        print(' Getting GeoCoding Info')
        final_df.append(coord(df, flag='GeoCoord', col_name='bbox_coords'))
    if sum(df['Flag_Loc'] == 'ReTweetLoc') > 0:
        print(' Processing RetweetLoc')
        final_df.append(retweetLoc(df, flag='ReTweetLoc', col_name='retweet_location'))
    if sum(df['Flag_Loc'] == 'UserProfileLoc') > 0:
        print(' Processing UserProfileLoc')
        final_df.append(retweetLoc(df, flag='UserProfileLoc', col_name='location'))

    final_df = pd.concat(final_df, axis=0, sort=False)

    return final_df
