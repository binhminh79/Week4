import csv

from . import GoogleCloud as gg
from . import postgres as pg
import psycopg2
import psycopg2.extras
import os



def extract_postgres_to_gcs(conn_dict, table_name, bucket_name, filename_format):
    uploader = gg.GoogleCloudHandler(gg.KEY_FILE, bucket_name)
    connector = pg.PostgreConnection(conn_dict)
    sql = f"SELECT * from {table_name}"
    dir_name = os.path.dirname(filename_format)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    csvfile = None
    try:
        records = []
        connector.execute(sql, True)
        field_names = []
        #column_names = ",".join(desc[0] for desc in pg.CONNECTOR.postgres_cursor.description) + "\n"
        for desc in connector.postgres_cursor.description:
            field_names.append(desc.name)

        i = 0
        file_counter = 1
        rows = []
        for row in connector.postgres_cursor.fetchall():
            if i == 0:
                file_id = str(file_counter).zfill(4)
                remote_path = filename_format.format(id=file_id)
                # = filename_format.format(id=file_id)
                csvfile = open(os.path.abspath(remote_path), mode="w", newline="\n")
                cvs_writer = csv.DictWriter(csvfile, field_names)
                cvs_writer.writeheader()
                 #csvfile.writelines(column_names)
            #csvfile.writelines( ", ".join(str(item) for item in row) + "\n")
            row_dict = dict(row)
            cvs_writer.writerow(row_dict)
            i += 1
            if i == 201:
                i = 0
                file_counter += 1
                finalize_file(csvfile, remote_path, uploader)

    except psycopg2.Error  as error:
        print("Failed to read data from table", error)

    finally:
        connector.close_connection()
        print("The connection is closed")
    if csvfile != None:
        finalize_file(csvfile, remote_path, uploader)
    return

#upload and delete file
def finalize_file(csvfile, remotefile, uploader):
    filename = csvfile.name
    csvfile.close()
    uploader.upload(filename, remotefile)

    os.remove(filename)
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


# generate source_uirs variable base on bucket_name and file_name_format
#remote_path = filename_format.format(table_name = table_name)


