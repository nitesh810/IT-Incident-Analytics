from cloudevents.http import CloudEvent
import json
from google.cloud import bigquery, storage
import pandas as pd
import io

def gcs_to_bq(cloud_event: CloudEvent):
    data = cloud_event.data

    bucket_name = data["bucket"]
    file_name = data["name"]

    if not file_name.endswith(".csv") or not file_name.startswith("retail_"):
        print(f"Skipping file: {file_name}")
        return

    table_suffix = file_name[:-4]
    table_id = f"supply-gbl-ww-pd.supply_pd_id.{table_suffix}"

    storage_client = storage.Client()
    bq_client = bigquery.Client(project="supply-gbl-ww-pd")

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    csv_bytes = blob.download_as_bytes()

    df = pd.read_csv(io.BytesIO(csv_bytes))
    df["ingest_date"] = pd.to_datetime("today")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    try:
        bq_client.get_table(table_id)
        print(f"Appending to table {table_id}")
    except:
        schema = []
        for col in df.columns:
            if df[col].dtype == "object":
                dtype = "STRING"
            elif "int" in str(df[col].dtype):
                dtype = "INTEGER"
            elif "float" in str(df[col].dtype):
                dtype = "FLOAT"
            elif "datetime" in str(df[col].dtype):
                dtype = "TIMESTAMP"
            else:
                dtype = "STRING"

            schema.append(bigquery.SchemaField(col, dtype))

        bq_client.create_table(bigquery.Table(table_id, schema=schema))
        print(f"Created table {table_id}")

    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"✅ Loaded {len(df)} rows into {table_id}")