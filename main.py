import os
import traceback
import pandas as pd
import functions_framework
from flask import jsonify
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID")
BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID")
BQ_TABLE_ID   = os.environ.get("BQ_TABLE_ID")

bq_client = bigquery.Client(project=BQ_PROJECT_ID)  # uses Cloud Run service account

@functions_framework.cloud_event
def gcs_trigger_process(cloud_event):
    try:
        print("=== EVENT RECEIVED ===")
        data = cloud_event.data or {}
        print("CloudEvent type:", getattr(cloud_event, "type", None))
        print("CloudEvent subject:", getattr(cloud_event, "subject", None))
        print("Event data keys:", list(data.keys()))
        print("Event data (first 2000 chars):", str(data)[:2000])
        bucket = data.get("bucket")
        name = data.get("name")
        

        # Basic validation
        if not bucket or not name:
            print("ERROR: Missing bucket or name in event payload:", data)
            return "Bad event payload: missing bucket or name", 400

        # Filter checks
        if not name.startswith("raw_data/"):
            print("SKIP: not in raw_data/ ->", name)
            return "", 204

        if not name.lower().endswith(".csv"):
            print("SKIP: not a .csv ->", name)
            return "", 204

        # Env var checks
        print("=== ENV VARS ===")
        print("BQ_PROJECT_ID:", BQ_PROJECT_ID)
        print("BQ_DATASET_ID:", BQ_DATASET_ID)
        print("BQ_TABLE_ID:", BQ_TABLE_ID)

        if not (BQ_PROJECT_ID and BQ_DATASET_ID and BQ_TABLE_ID):
            print("ERROR: Missing one or more BigQuery env vars.")
            return "Missing env vars: BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID", 500

        table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
        print("Target table_ref:", table_ref)

        # Read CSV from GCS
        uri = f"gs://{bucket}/{name}"
        print("Reading CSV from:", uri)

        df = pd.read_csv(uri)
        print("CSV read OK. Rows:", len(df), "Cols:", len(df.columns))
        print("Columns:", list(df.columns)[:50])

        # Check table existence
        table_exists = True
        try:
            bq_client.get_table(table_ref)
            print("Table exists already:", table_ref)
        except NotFound:
            table_exists = False
            print("Table does NOT exist yet. It should be created now:", table_ref)

        # Load to BigQuery
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_APPEND
                if table_exists
                else bigquery.WriteDisposition.WRITE_TRUNCATE  # first time: create+write
            )
            # create_disposition defaults to CREATE_IF_NEEDED
        )

        print("Starting BigQuery load job...")
        load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)

        print("Load job id:", load_job.job_id)
        result = load_job.result()  # wait until done
        print("BigQuery load finished. Job state:", load_job.state)

        # Verify rows in destination table
        dest_table = bq_client.get_table(table_ref)
        print("Destination table row count (approx):", dest_table.num_rows)

        return jsonify({
            "status": "success",
            "file": name,
            "rows_loaded": len(df),
            "bq_table": table_ref,
            "table_created_now": (not table_exists),
            "job_id": load_job.job_id
        }), 200

    except Exception as e:
        print("=== ERROR OCCURRED ===")
        print("Error:", str(e))
        print("Traceback:")
        print(traceback.format_exc())

        # IMPORTANT: raise so Cloud Run marks it as failed (useful for Eventarc retries)
        raise
