import os
from google.oauth2.service_account import Credentials
from google.cloud import storage as ggstorage
from google.cloud import bigquery

class GoogleCloudHandler:
    def __init__(self, key_file, bucket_name = None):
        self.__key_file = key_file
        self.bucket_name = bucket_name
        ggcredentials = Credentials.from_service_account_file(key_file)
        self.__storage_client = ggstorage.Client(credentials = ggcredentials)
        self.bigquery_client = bigquery.Client(credentials = ggcredentials)
        self.__bucket = None
        return

    def ensure_bucket(self):
#        self.__bucket = ggstorage.bucket.Bucket(self.__storage_client, self.bucket_name)
        self.__bucket = self.__storage_client.bucket(self.bucket_name)
        if self.__bucket == None or not self.__bucket.exists():
            self.__bucket = self.__storage_client.create_bucket(self.bucket_name)
            print(f"Bucket {self.__bucket.name} created.")
        return

    def upload(self, file_name, remote_path):
        if self.__bucket == None:
            self.ensure_bucket()
        if self.__bucket == None:
            print(f"Cannot create bucket {self.bucket_name}")
            return

        #upload_filename = f"{remote_path}{os.path.basename(file_name)}"  # the name of blob

        blob = self.__bucket.blob(remote_path)
        blob.upload_from_filename(file_name)

        print(f"File {blob.name} is uploaded.")
        return

DATA_DIR = "../../data"
KEY_FILE = os.path.expanduser("~/Documents/projects/data-analytics-engineer-minh-0b2185ce811c.json")

