# Source:
# https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python
import pickle
import yaml
import pandas as pd
from azure.storage.blob import BlobClient, BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
import glob
import os
import sys

DATABASE = ''
CONNECTION_STR = ''
CONTAINER = ''
TEMP_PATH = ''

def load_config(config_path):
    global DATABASE, CONNECTION_STR, CONTAINER, TEMP_PATH

    yaml_file = open(config_path)
    config = yaml.load(yaml_file, Loader=yaml.FullLoader)
    DATABASE = os.environ['DATABASE'] = config['database']

    if DATABASE == 'azure':
        PATH = os.environ['PATH'] = config['azure_path']
        CONNECTION_STR = os.environ['CONNECTION_STR'] = config['connection_string']
        CONTAINER = os.environ['CONTAINER'] = config['container']
    elif DATABASE == 'local':
        PATH = os.environ['PATH']  = config['local_path']

    os.environ['PROCESSED_PATH'] = PATH + '/processed'
    os.environ['WEEKLY_DATA_PATH'] = PATH + '/weekly_data'
    os.environ['FLAT_FILES_PATH'] = PATH + '/FlatFiles'
    os.environ['DAILY_DATA_PATH'] = PATH + '/aggregate'
    TEMP_PATH = os.environ['TEMP_PATH'] = './intermediary'
    os.environ['GOOGLE_API_KEY'] = config['google_api_key']


    if DATABASE == 'local':
        for folder in ['PROCESSED_PATH', 'WEEKLY_DATA_PATH', 'FLAT_FILES_PATH', 'DAILY_DATA_PATH']:
            os.makedirs(os.environ[folder], exist_ok=True)
    elif DATABASE == "azure":
        os.makedirs(TEMP_PATH, exist_ok=True)


def store_file(data, filepath, filename, sep=',', mode='w', header=True):
    file_format = filename.split('.')[-1]
    if DATABASE == 'local':
        if file_format == 'csv':
            data.to_csv(f'{filepath}/{filename}', index=False, sep=sep, mode=mode, header=header)
        elif file_format == 'pkl':
            with open(f"{filepath}/{filename}", 'wb') as f:
                pickle.dump(data, f)
        elif file_format == 'txt':
            with open(f"{filepath}/{filename}", 'w') as f:
                f.write(data)
    elif DATABASE == 'azure':
        if file_format == 'csv':
            output = data.to_csv(sep=sep, index=False, mode=mode, header=header)
            blob = BlobClient.from_connection_string(conn_str=CONNECTION_STR,
                                                     container_name=CONTAINER,
                                                     blob_name=f"{filepath}/{filename}")
            if header:
                blob.create_append_blob()
                blob.upload_blob(output, overwrite=True, blob_type='AppendBlob')
            else:
                blob.upload_blob(output, overwrite=False, blob_type='AppendBlob')
        elif file_format == 'pkl':
            blob = BlobClient.from_connection_string(conn_str=CONNECTION_STR,
                                                     container_name=CONTAINER,
                                                     blob_name=f"{filepath}/{filename}")
            with open(f"{TEMP_PATH}/PickleFile.pkl", 'wb') as f:
                pickle.dump(data, f)
            with open(f"{TEMP_PATH}/PickleFile.pkl", 'rb') as data:
                blob.upload_blob(data, overwrite=True)
        elif file_format == 'txt':
            blob = BlobClient.from_connection_string(conn_str=CONNECTION_STR,
                                                     container_name=CONTAINER,
                                                     blob_name=f"{filepath}/{filename}")
            with open(f"{TEMP_PATH}/TextFile.txt", 'w') as f:
                f.write(data)
            with open(f"{TEMP_PATH}/TextFole.txt", 'r') as data:
                blob.upload_blob(data, overwrite=True)

def retrieve_file(filepath, filename, sep=',', usecols=None, skiprows=None):
    file_format = filename.split('.')[-1]
    data = False
    try:
        if DATABASE == 'local':
            if file_format == 'csv':
                data = pd.read_csv(f'{filepath}/{filename}', sep=sep, usecols=usecols, skiprows=skiprows)
            elif file_format == 'xls':
                data = pd.read_excel(f'{filepath}/{filename}', sep=sep, usecols=usecols, skiprows=skiprows)
            elif file_format == 'pkl':
                with open(f"{filepath}/{filename}", 'rb') as f:
                    data = pickle.load(f)
            elif file_format == 'txt':
                data = open(f"{filepath}/{filename}", 'r')
        elif DATABASE == 'azure':
            if file_format == 'csv':
                blob = BlobClient.from_connection_string(conn_str=CONNECTION_STR,
                                                         container_name=CONTAINER,
                                                         blob_name=f"{filepath}/{filename}")
                with open(TEMP_PATH + "/CSVFile.csv", "wb") as my_blob:
                    blob_data = blob.download_blob()
                    blob_data.readinto(my_blob)

                data = pd.read_csv(TEMP_PATH + "/CSVFile.csv", sep=sep, usecols=usecols, skiprows=skiprows)
            elif file_format == 'xls':
                blob = BlobClient.from_connection_string(conn_str=CONNECTION_STR,
                                                         container_name=CONTAINER,
                                                         blob_name=f"{filepath}/{filename}")
                with open(TEMP_PATH + "/ExcelFile.xls", "wb") as my_blob:
                    blob_data = blob.download_blob()
                    blob_data.readinto(my_blob)

                data = pd.read_excel(TEMP_PATH + "/ExcelFile.xls", sep=sep, usecols=usecols, skiprows=skiprows)
            elif file_format == 'pkl':
                blob = BlobClient.from_connection_string(conn_str=CONNECTION_STR,
                                                         container_name=CONTAINER,
                                                         blob_name=f"{filepath}/{filename}")
                with open(f"{TEMP_PATH}/PickleFile.pkl", 'wb') as my_blob:
                    blob_data = blob.download_blob()
                    blob_data.readinto(my_blob)

                with open(f"{TEMP_PATH}/PickleFile.pkl", 'rb') as f:
                    data = pickle.load(f)
            elif file_format == 'txt':
                blob = BlobClient.from_connection_string(conn_str=CONNECTION_STR,
                                                         container_name=CONTAINER,
                                                         blob_name=f"{filepath}/{filename}")
                with open(f"{TEMP_PATH}/TextFile.txt", 'wb') as my_blob:
                    blob_data = blob.download_blob()
                    blob_data.readinto(my_blob)

                data = open(f"{TEMP_PATH}/TextFile.txt", 'r')
        return data
    except Exception as e:
        print(e)
        sys.exit(0)


def list_files(filepath, format='csv'):
    if DATABASE == 'local':
        filenames = list(map(os.path.basename, glob.glob(f"{filepath}/*.{format}")))
    elif DATABASE == 'azure':
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STR)
        container_client = blob_service_client.get_container_client(CONTAINER)
        blob_list = container_client.list_blobs(f"{filepath}/")
        filenames = [os.path.basename(blob.name) for blob in blob_list]

    return filenames


def exists(file):
    if DATABASE == "azure":
        blob = BlobClient.from_connection_string(conn_str=CONNECTION_STR,
                                                 container_name=CONTAINER,
                                                 blob_name=file)
        try:
            blob.get_blob_properties()
        except ResourceNotFoundError:
            return False
        return True
    elif DATABASE == "local":
        return os.path.exists(file)
