import os
import psycopg2
import time
import pandas as pd
from . import GoogleCloud as gg
from . import postgres as pg

def extract_postgres_to_gcs(conn_dict, table_name, bucket_name, filename_format):
    uploader = gg.GoogleCloudHandler(gg.KEY_FILE, bucket_name)
    connector = pg.PostgreConnection(conn_dict)
    connector.connect()
    dir_name = os.path.dirname(filename_format)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    file_id = str(1).zfill(4)
    remote_path = filename_format.format(id=file_id)
    csvfile = os.path.abspath(remote_path)

    # query and import as Dataframe
    df = pd.read_sql(f"SELECT * from {table_name}", connector.postgres_connection)

    # add here your cleaning operations
    df = df.dropna(axis=0)
    # export
    df.to_csv(csvfile, index=False)
    uploader.upload(csvfile, remote_path)
    #os.remove(csvfile)
    connector.close_connection()
    return

# Write the code to load data from source_uris to table table_name in dataset with write_disposition
# write disposition must be WRITE_EMPTY, WRITE_APPEND OR WRITE_TRUNCATE, other will print error
# Minh: schema added in case we want to specify schema
def load_data_from_gcs_to_bigquery (source_uris, dataset, table_name, write_disposition=gg.bigquery.WriteDisposition.WRITE_APPEND,
                     schema=None):
    uploader = gg.GoogleCloudHandler(gg.KEY_FILE)
    bigquery = gg.bigquery
    if write_disposition not in (bigquery.WriteDisposition.WRITE_APPEND,
                                 bigquery.WriteDisposition.WRITE_EMPTY,
                                 bigquery.WriteDisposition.WRITE_TRUNCATE):
        print(
            f"write_disposition = \"{write_disposition}\", which is not WRITE_EMPTY, WRITE_APPEND OR WRITE_TRUNCATE, nothing uploaded to bigquery")
        return False

    bq_schema = None
    if schema != None:
        bq_schema = list()
        for key, val in schema.items():
            bq_schema.append(bigquery.schema.SchemaField(key, val))
    table_id = f"{dataset}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=write_disposition,
        schema=bq_schema,
    )
    job = uploader.bigquery_client.load_table_from_uri(
        source_uris=source_uris,
        destination=table_id,
        job_config=job_config
    )
    job.result()  # Waits for the job to complete.

    return True

