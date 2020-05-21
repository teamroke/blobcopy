"""
    Author: Kevin MacPherson <kevin@teamroke.dev>
    Created: May 20th 2020
"""

import sqlite3
import logging
import os
from hashlib import md5
from azure.storage.blob import BlobServiceClient

DEFAULT_PATH = os.path.join(os.path.dirname(__file__), 'blobcopy.sqlite3')
DEFAULT_CONTAINER = 'teamrokecontainer'


def get_database(db_path=DEFAULT_PATH):
    db_connection = sqlite3.connect(db_path)
    return db_connection


def create_tables(db_conn):
    db_cursor = db_conn.cursor()
    blob_table_sql="""
    CREATE TABLE blobs (
        blob_name PRIMARY KEY,
        blob_md5 text NOT NULL,
        date_added date NOT NULL
    )
    """
    db_cursor.execute(blob_table_sql)


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


def scheduler():
    db_conn = get_database()
    bsc = get_blob_service_client()
    cc = get_container_client(bsc)
    blob_list = get_blob_list(cc)
    for blob in blob_list:
        blob_name, blob_md5, blob_create_date = get_blob_info(bsc, blob)
        print(blob_name)
        print(blob_md5)
        print(blob_create_date)




if __name__ == '__main__':
    scheduler()