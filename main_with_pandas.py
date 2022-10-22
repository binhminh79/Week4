from lib.postgresql import etl_helper_with_pandas
from google.cloud import bigquery

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

CONNECTION_PARAMS = {'database': "data-analytics-engineer", 'user': 'postgres', 'password': 'postgres', 'host': '127.0.0.1', 'port': '5432'}
# Declare bucket_name to upload data
bucket_name = "data-analytics-engineer-minh"
table_schemas = {'employees': None, 'departments':{'dept_no' : 'STRING', 'dept_name' : 'STRING'}, 'dept_emp' : None}

filename_format = "data3/{table_name}/{{id}}.csv"
# Declare dataset
dataset = "sql_practice"

for table_name, schema in table_schemas.items():
    one_name = filename_format.format(table_name = table_name)
    etl_helper_with_pandas.extract_postgres_to_gcs(CONNECTION_PARAMS, table_name = table_name, bucket_name=bucket_name,
                                    filename_format = one_name)
    source_uris = f"gs://{bucket_name}/{one_name.format(id='*')}"
    # declare write_disposition
    bigquery_write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    etl_helper_with_pandas.load_data_from_gcs_to_bigquery(
        source_uris=source_uris,
        dataset=dataset,
        table_name=table_name,
        write_disposition=bigquery_write_disposition,
        schema = schema
    )
    break
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
