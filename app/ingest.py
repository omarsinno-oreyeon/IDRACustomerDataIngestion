"""
Set the data on S3, and in the database.
"""

# Standard imports
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dateutil import parser
import logging
import json
import csv
import os

import argparse

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger()
logger.setLevel("INFO")

# Third party imports
from botocore.exceptions import ClientError
import boto3

# Custom imports
from online_db import set_connection, execute_query

s3_client = boto3.client("s3")

def upload_object(bucket_name: str, prefix: str, file_path: str) -> None:
    """
    Upload a single object to S3.

    Args:
        bucket_name (str): Name of the bucket.
        prefix (str): Name of the prefix.
        file_path (str): The local path to the image file.

    Returns:
        None
    """
    with open(file_path, "rb") as f:
        file_data = f.read()

    file_name = os.path.basename(file_path)
    key = os.path.join(prefix, file_name)

    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=file_data)
    except ClientError as e:
        logger.info(f"Failed to upload {file_path} to S3: {e}")
        raise e

def ingest_to_s3(
        data_path: str, bucket_name: str, prefix: str, max_workers: int = 5
    ) -> None:
    """
    Ingest data to S3.

    Args:
        data_path (str): The local path to the data file.
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix where the objects will be located.
        max_workers (int): The maximum number of worker threads to use for uploading.
    Returns:
        None
    """
    # Unpack the images within the directory
    images = os.listdir(data_path)

    logger.info(f"Uploading {len(images)} images to S3 bucket {bucket_name}/{prefix}")

    # Upload images concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for img in images:
            img_path = os.path.join(data_path, img)
            future = executor.submit(upload_object, bucket_name, prefix, img_path)
            futures[future] = img

        for future in as_completed(futures):
            result = future.result()

    # Log completion
    logger.info("Completed uploading images to S3.")

def ingest_to_db(field_mapping_path: str, default_values_path: str, run_id: int, bucket_name: str, prefix: str, user_id: int):
    """
    Ingest data to the database.

    Args:
        field_mapping_path (str): Path to the field mapping configuration.
        default_values_path (str): Path to the default values configuration.
        run_id (int): Run ID, maps to Report ID
        bucket_name (str): Name of the bucket we ingested to
        prefix (str): Prefix in the bucket we ingested to where the data is located
        user_id (int): NERVA User
    Returns:
        None
    """
    # Establish db connection
    conn = set_connection()

    # Fetch the field mapping
    with open(field_mapping_path, "r") as f:
        field_mapping = json.load(f)

    # Fetch the default values
    with open(default_values_path, "r") as f:
        default_values = json.load(f)

    # Load runs and fod data information from CSV
    fod_data_csv_path = os.path.join("app/offline-db/csv", f"fodDataIdra_run_{run_id}.csv")
    runs_data_csv_path = os.path.join("app/offline-db/csv", f"runsIdra_run_{run_id}.csv")

    items = []

    with open(fod_data_csv_path, "r") as fod_csv_file:
        fod_csv_reader = csv.DictReader(fod_csv_file)

        for row in fod_csv_reader:
            items.append(row)

    with open(runs_data_csv_path, "r") as runs_csv_file:
        runs_csv_reader = csv.DictReader(runs_csv_file)
        runs_data = list(runs_csv_reader)

    # Insert report in tblReport
    report_runs_data = list(
        filter(
            lambda entry: entry["ID"] == str(run_id), runs_data
        )
    )[0]

    start_time = report_runs_data["startTime"]
    parsed_start_time = parser.parse(start_time)

    end_time = report_runs_data["endTime"]
    parsed_end_time = parser.parse(end_time)

    report_data = {
        "reportID": "",
        "userID": user_id,
        "reportType": report_runs_data["reportType"].capitalize(),
        "unit": report_runs_data["unit"],
        "numberOfOfficers": int(report_runs_data["nbreOfOfficers"]),
        "numberOfSNCO": int(report_runs_data["snco"]),
        "numberOfEnlistedOfficers": int(report_runs_data["enlisted"]),
        "numberOfFods": int(report_runs_data["fodCount"]),
        "startTime": parsed_start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "endTime": parsed_end_time.strftime("%Y-%m-%d %H:%M:%S")
    }

    # Removed reportID because it auto increments
    del report_data["reportID"]

    string_parameters = ["%s"]*len(report_data)
    string_parameters = ", ".join(string_parameters)

    string_cols = list(report_data.keys())
    string_cols = ", ".join(string_cols)

    insert_report_query = f"INSERT INTO tblReport ({string_cols}) VALUES ({string_parameters});"

    params = list(report_data.values())

    report_insert_response = execute_query(conn, insert_report_query, params)

    fetch_report_id_query = "SELECT reportID FROM tblReport WHERE userID = %s ORDER BY reportID DESC LIMIT 1;"
    report_fetch_response = execute_query(conn, fetch_report_id_query, [user_id])
    report_fetch_response = json.loads(report_fetch_response)

    report_id = report_fetch_response[0]["reportID"]

    logger.info(f"Report ID in the online DB is {report_id}.")

    # Map report ID
    report_id_map_json_path = "app/online-db/mappings"
    os.makedirs(report_id_map_json_path, exist_ok=True)

    report_id_map_json = os.path.join(
        report_id_map_json_path,
        f"report-offline-{run_id}-online-{report_id}.json"
    )

    # JSON file that maps offline to online
    with open(report_id_map_json, "w") as rimj:
        json.dump({run_id: report_id}, rimj)

    # Set sample record that will be modified for each record
    sample_data = {
        "fodID": "",
        "reportID": "",
        "fodImageName": "",
        "fodImageUri": "",
        "locationLatitude": "",
        "locationLongitude": "",
        "Comment": "",
        "source": "",
        "finalSource": "",
        "color": "",
        "finalColor": "",
        "material": "",
        "finalMaterial": "",
        "size": "",
        "finalSize": "",
        "bbx": "",
        "bby": "",
        "bbw": "",
        "bbh": "",
        "fodModelClassificationID": "",
        "fodModelClassificationFinal": "",
        "fodModelClassificationInferenceTime": "",
        "fodModelDetectionInferenceTime": "",
        "numberOfRetries": "",
        "phoneToGroundDistInCm": "",
        "topPrediction1": "",
        "topPrediction2": "",
        "topPrediction3": "",
        "emptyAlbumId": "",
        "createdTime": "",
    }

    data = []

    # Set bucket and prefix where images are located
    ordered_fod_id_ls = []

    for item in items:
        record = sample_data.copy()

        # Set FOD Image Name
        image_name = item["imageName"]
        record["fodImageName"] = image_name

        # Set FOD Image URI
        record["fodImageUri"] = f"{bucket_name}/{prefix}/{image_name}"

        # Create a map that maps FOD ID from offline DB to online DB
        record["fodID"] = item["ID"]
        ordered_fod_id_ls.append(item["ID"])

        # Set reportID
        record["reportID"] = report_id

        # Go over field maps
        for field, mapping in field_mapping.items():

            # Sets the record key with the online field name,
            # by fetching the value from the data with the offline field name

            try:
                record[mapping] = item[field]
            except KeyError:

                # If not in fodData, info is probably in runsData
                # logger.info(f"{field} information in runs data, not in fods data.")
                record_run = list(filter(lambda entry: entry["ID"] == str(run_id), runs_data))[0]

                record[mapping] = record_run[field]

        # Go over initialized fields
        for field, init in default_values.items():
            if field in field_mapping.values():
                idx = list(field_mapping.values()).index(field)
                field = list(field_mapping.keys())[idx]

            record[field] = item.get(field, init)

        # Temporary solution to removing additionally available longitude and latitude information
        del record["longitude"]
        del record["latitude"]
        del record["fodID"]

        # Check missing fields to be filled
        predictions = [
            "topPrediction1",
            "topPrediction2",
            "topPrediction3",
            "createdTime",
        ]

        for pred in predictions:
            record[pred] = item.get(pred, "NULL")

        # Set formatted date time
        created_time = item["createdTime"]

        # Parse the datetime regardless of its format
        parsed_datetime = parser.parse(created_time)
        record["createdTime"] = parsed_datetime.strftime("%Y-%m-%d %H:%M:%S")

        # Format finalSize to remove the metric ("1.7 in" --> 1.7)
        record["finalSize"] = float(record["finalSize"].split(" ")[0])

        # Append record to data list: replace empty strings with None
        record = {k: (None if v == "" else v) for k,v in record.items()}

        data.append(record)

    # TODO: How many entries per query? Set threshold?
    field_params = list(data[0].keys())
    field_params = ", ".join(field_params)

    string_params = ["%s"]*len(record)
    string_params = ", ".join(string_params)

    fod_insert_query = f"INSERT INTO tblFod ({field_params}) VALUES({string_params})"

    fod_insert_response = execute_query(
        conn,
        fod_insert_query,
        [list(e.values()) for e in data],
        execution_type="many"
    )

    fetch_fod_id_query = "SELECT fodID FROM tblFod WHERE reportID = %s ORDER BY fodID ASC;"
    fod_id_fetch_response = execute_query(conn, fetch_fod_id_query, [report_id])
    fod_id_fetch_response = json.loads(fod_id_fetch_response)

    # Using the image names find the fodIDs and report IDs and create maps
    fod_id_map_path = "app/online-db/mappings"
    os.makedirs(fod_id_map_path, exist_ok=True)

    fod_id_map_json = os.path.join(
        fod_id_map_path,
        f"fods-offline-{run_id}-online-{report_id}.json"
    )

    # JSON file that maps offline to online
    with open(fod_id_map_json, "w") as fimj:
        fid_map = {
            offline_fid: online_fid["fodID"] for offline_fid, online_fid in zip(
                ordered_fod_id_ls,
                fod_id_fetch_response
            )
        }

        json.dump(fid_map, fimj)

    conn.close()

if __name__ == "__main__":
    # Run IDRA Data ingestion process
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("--run-id", required=True, help="Run number from offline db", type=int)
    argument_parser.add_argument("--bucket-name", required=True, default="idra-commercial", help="Bucket name", type=str)
    argument_parser.add_argument("--user-id", required=True, default=333, help="User ID set up for Nerva", type=int)

    args = argument_parser.parse_args()

    # Unpack args
    bucket_name = args.bucket_name
    user_id = args.user_id
    run_id = args.run_id

    data_path = f"app/images/run_{run_id}/"
    prefix = f"FOD Images/"

    # If already ingested to S3, skip pushing data to S3
    try:
        listed_images = os.listdir(data_path)

        image_key = f"{prefix}{listed_images[0]}"
        s3_client.head_object(Bucket=bucket_name, Key=image_key)

        logger.info(f"Data is already available in {bucket_name}, {image_key}")

    except ClientError as ce:
        logger.info(f"Object not in URI {bucket_name}{prefix}")

        ingest_to_s3(data_path, bucket_name, prefix)

        raise ce

    except Exception as e:
        logger.error(f"Error looking up image in Bucket: {bucket_name} Prefix {prefix}")
        raise e

    # If data already in db, skip pushing data to db
    # Push metadata to database
    field_mapping_path = "app/offline-db/mappings/field-map.json"
    default_values_path = "app/offline-db/mappings/default-values.json"

    try:
        map_prefix = f"FOD-Images-Map/run_{run_id}/"

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=map_prefix,
            MaxKeys=1
        )

    except ClientError as ce:
        logger.error(f"Error listing objects in map prefix for run {run_id}.")
        raise ce

    if response.get("KeyCount", 0) == 0:
        ingest_to_db(field_mapping_path, default_values_path, run_id, bucket_name, prefix, user_id)

        # Push JSON mappings to S3
        mappings = os.listdir("app/online-db/mappings/")

        for mp in mappings:
            mp_path = os.path.join("app/online-db/mappings/", mp)

            upload_object(
                bucket_name,
                f"FOD-Images-Map/run_{run_id}/",
                mp_path
            )
    else:
        logger.info(f"Data for report {run_id} already ingested in the database. Please verify.")
