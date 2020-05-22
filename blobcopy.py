"""
    Author: Kevin MacPherson <kevin@teamroke.dev>
    Created: May 20th 2020
"""

import sqlite3
import logging
import os
from time import sleep
from azure.storage.blob import BlobServiceClient

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), 'blobcopy.sqlite3')
DEFAULT_CONTAINER = 'teamrokecontainer'
DEFAULT_LOG_DIRECTORY = os.path.join(os.getcwd(), 'log')
DEFAULT_LOG_FILE = 'blobcopy.log'
DEFAULT_DOWNLOAD_LOCATION = os.path.join(os.getcwd(), 'download')


def get_logger(log_location=DEFAULT_LOG_DIRECTORY, logfile=DEFAULT_LOG_FILE):
    logging.getLogger(__name__)
    if not os.path.exists(log_location):
        os.mkdir(log_location)
    logfile = os.path.join(log_location, logfile)
    logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename=logfile,
                        level=logging.INFO)
    logging.info('Application started')


def get_database(db_path=DEFAULT_DB_PATH):
    db_connection = sqlite3.connect(db_path)
    return db_connection


def create_tables(db_conn):
    insert_cursor = db_conn.cursor()
    blob_table_sql = """
    CREATE TABLE blobs (
        blob_name text PRIMARY KEY,
        blob_md5 text NOT NULL,
        date_added date NOT NULL
    )
    """
    insert_cursor.execute(blob_table_sql)


def get_blob_service_client():
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    return blob_service_client


def get_container_client(blob_service_client, container_name=DEFAULT_CONTAINER):
    container_client = blob_service_client.get_container_client(container_name)
    return container_client


def get_blob_list(container_client):
    blob_list = container_client.list_blobs()
    return_list = []
    for blob in blob_list:
        return_list.append(blob.name)
    return return_list


def get_blob_info(blob_service_client, blob_name, container_name=DEFAULT_CONTAINER):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_properties = blob_client.get_blob_properties()
    blob_name = blob_properties.name
    blob_md5 = blob_properties.content_settings.content_md5
    blob_date = blob_properties.last_modified
    return blob_name, blob_md5, blob_date


def blob_download(blob_service_client, blob_name, download_folder=DEFAULT_DOWNLOAD_LOCATION, container_name=DEFAULT_CONTAINER):
    if not os.path.exists(download_folder):
        os.mkdir(download_folder)
    download_file_name = os.path.join(download_folder, blob_name)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    with open(download_file_name, 'wb') as data:
        data.write(blob_client.download_blob().readall())


def main():
    get_logger()
    db_conn = get_database()
    # create_tables(db_conn)
    db_cursor = db_conn.cursor()
    bsc = get_blob_service_client()
    cc = get_container_client(bsc)
    while True:
        sleep(30)
        blob_list = get_blob_list(cc)
        for blob in blob_list:
            blob_name, blob_md5, blob_create_date = get_blob_info(bsc, blob)
            db_cursor.execute("SELECT * FROM blobs WHERE blob_name LIKE ?", (blob_name,))
            result = db_cursor.fetchone()
            logging.debug(result)
            if result is None:
                logging.info('New blob found: ' + blob_name)
                value = (blob_name, bytes(blob_md5), blob_create_date)
                sql = 'INSERT INTO blobs(blob_name,blob_md5,date_added) VALUES(?,?,?)'
                db_cursor.execute(sql, value)
                blob_download(bsc, blob_name)
                db_conn.commit()
            else:
                logging.info('Skipped known file: ' + blob_name)

    db_conn.close()


if __name__ == '__main__':
    main()
