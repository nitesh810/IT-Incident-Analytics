from google.cloud import dataproc_v1
from cloudevents.http import CloudEvent

PROJECT_ID = "my-gcp-project"
REGION = "us-central1"
SPARK_FILE = "gs://incident-bucket/spark/incident_job.py"
BQ_DATASET = "incident_ds"

def trigger_dataproc(file_path):

    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )

    batch = {
        "pyspark_batch": {
            "main_python_file_uri": SPARK_FILE,
            "args": [
                f"--input={file_path}",
                f"--dataset={BQ_DATASET}"
            ]
        },

        "runtime_config": {
            "version": "2.2"
        }
    }

    request = {
        "parent": f"projects/{PROJECT_ID}/locations/{REGION}",
        "batch": batch
    }

    response = client.create_batch(request=request)

    print("Dataproc Job:", response.name)
    return response.name


def main(cloud_event: CloudEvent):

    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]

    # only csv files
    if not name.endswith(".csv"):
        print("Ignored non-csv file:", name)
        return "ignored"

    file_path = f"gs://{bucket}/{name}"

    job = trigger_dataproc(file_path)

    return f"Triggered job: {job}"