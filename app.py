from flask import Flask, request
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
import shutil
import os
import requests
from requests.adapters import HTTPAdapter
import zipfile
from datetime import datetime
import pandas as pd
import json

app = Flask(__name__)

KEY_FILE = os.getenv("KEYFILE")

def authenticate(token):
    if token != os.getenv("PASSWORD"):
        raise Exception("Unautorizhed")
    

def get_column_type(column):
    x = str(column)
    if x == "int64":
        return "INTEGER"
    elif x == "float64":
        return "FLOAT"
    return "STRING"


def table_exists(client, table_id):
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False

def check_and_create_dataset(client, dataset_id):
    # Checks if Dataset exists and create it if not
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        client.create_dataset(dataset, timeout=10)


def load_file_to_bucket(client, local_path, bucket_name, path="monthly"):

    # Set the Storage client and Bucket
    bucket = client.get_bucket(bucket_name)

    # Create a list of Bucket files URI and clean
    bucket_uris = []
    for f in os.listdir(local_path):
        if not f.split(".")[-1].lower() in ["txt", "csv", "json"]:
            continue
        if path=="monthly":
            remote_path = f"{datetime.now().strftime('%Y%m')}/{f}"
        elif path=="daily":
            remote_path = f"{datetime.now().strftime('%Y%m%d')}/{f}"

        blob = bucket.blob(remote_path)
        df = pd.read_csv(os.path.join(local_path, f))
        with open(os.path.join(local_path, f), "rb") as b:
            blob.upload_from_file(b)
        data_types = [get_column_type(df[c].dtype) for c in df.columns]
        headers = {x: y for x, y in zip(list(df.columns), data_types)}
        bucket_uris.append({
            "name": f.split(".")[0],
            "remote_path": remote_path,
            "headers": headers,
            "uri": f"gs://{bucket_name}/{remote_path}"
        })
    if not bool(bucket_uris):
        raise Exception("bucket_uris is empty")

    print(bucket_uris)
    print("File uploaded to bucket")

    return bucket_uris


def load_file_to_table(client, element, table_id, f="csv"):
    if f=="json" :
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True
        )  # Create JSON job config
    elif f=="csv":
        # Create a list of SchemaFields (STRING by default)
        element_schema = [bigquery.SchemaField(
            x, y) for x, y in element["headers"].items()]
        job_config = bigquery.LoadJobConfig(
            schema=element_schema,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )  # Create the standard job config

    # Get the URI of the file in bucket
    element_uri = element["uri"]

    # Load the file on BigQuery
    load_job = client.load_table_from_uri(
        element_uri, table_id, job_config=job_config
    )

    # Result
    load_job.result()

    # Get and print the successful rows
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


@app.route("/cl/red/monthly_data/", methods=["GET"])
def monthty_data():
    token = request.headers.get("Token")
    authenticate(token)
        
    # Get parameters
    BUCKET_NAME = request.args.get("bucket")
    DATASET_ID = request.args.get("dataset_id")

    # Set request option
    MAX_RETRIES = 10
    # os.getenv("MONTHLY_DATA")
    URI = "https://datos.gob.cl/api/action/package_show?id=33245"

    # Do the request
    session = requests.Session()
    session.mount(URI, HTTPAdapter(max_retries=MAX_RETRIES))
    response = session.get(URI, timeout=30)

    # Get the data from response
    data = response.json()
    if data["success"] != True:
        return f"Doesn't was response from {URI}", 500
    resources = data["result"]["resources"]
    download_uri = resources[0]["url"]
    download_filename = download_uri.split("/")[-1]

    # Download and write the file
    download = session.get(download_uri)
    with open(download_filename, "wb") as f:
        f.write(download.content)

    # Extract the files and clean
    unzip_path = "unzip"
    if os.path.isdir(unzip_path):
        shutil.rmtree(unzip_path)
    os.makedirs(unzip_path, exist_ok=True)
    with zipfile.ZipFile(download_filename, "r") as zip_file:
        zip_file.extractall(unzip_path)
    os.remove(download_filename)

    # Set the Storage client and Bucket
    client = storage.Client.from_service_account_json(KEY_FILE)

    # bucket = client.get_bucket(BUCKET_NAME)

    # # Create a list of Bucket files URI and clean
    # bucket_uris = []
    # for f in os.listdir(unzip_path):
    #     if f.split(".")[-1].lower() != "txt":
    #         continue
    #     remote_path = datetime.now().strftime("%Y%m") + f"/{f}"
    #     blob = bucket.blob(remote_path)
    #     df = pd.read_csv(os.path.join(unzip_path, f))
    #     with open(os.path.join(unzip_path, f), "rb") as b:
    #         blob.upload_from_file(b)
    #     data_types = [get_column_type(df[c].dtype) for c in df.columns]
    #     headers = {x:y for x,y in zip(list(df.columns), data_types)}
    #     bucket_uris.append({
    #         "name":f.split(".")[0],
    #         "remote_path": remote_path,
    #         "headers": headers,
    #         "uri":f"gs://{BUCKET_NAME}/{remote_path}"
    #     })

    client = storage.Client.from_service_account_json(KEY_FILE)

    # Get bucket uris
    bucket_uris = load_file_to_bucket(client, unzip_path, BUCKET_NAME)
    shutil.rmtree(unzip_path)

    # Create a BigQuery client and load every csv file in bucket
    client = bigquery.Client.from_service_account_json(KEY_FILE)

    # Create dataset if not exists
    check_and_create_dataset(client, DATASET_ID)


    for element in bucket_uris:
        print("ELEMENT:", element, "\n\n\n")

        # Set tables ids
        table_id = ".".join([DATASET_ID, datetime.now().strftime(
            f'%Y%m-{element["name"]}')])  # Get current table_id

        # delete table if exists
        if table_exists(client, table_id):
            client.delete_table(table_id)

        # Load the data
        load_file_to_table(client, element, table_id)

    return f"File loaded successfully", 200


@app.route("/cl/red/daily_data/", methods=["GET"])
def daily_data():
    # Get parameters
    token = request.headers.get("Token")
    authenticate(token)

    BUCKET_NAME = request.args.get("bucket")
    DATASET_ID = request.args.get("dataset_id")
    RECORRIDO_ID = request.args.get("recorrido_id")

    # Set request option
    MAX_RETRIES = 10
    URI = "https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint="

    # Do the request
    session = requests.Session()
    session.mount(URI+RECORRIDO_ID, HTTPAdapter(max_retries=MAX_RETRIES))
    response = session.get(URI+RECORRIDO_ID, timeout=60)

    data = response.json()
    json_path = "daily"
    os.makedirs(json_path, exist_ok=True)
    json_filename = f'daily_{RECORRIDO_ID}.json'
    with open(json_filename, 'w') as f:
        json.dump(data, f)

    # Create pandas dataset
    df = pd.read_json(json_filename)

    for k in ["ida", "regreso"]:
        # Paraderos
        for i in df[k]["paraderos"]:
            # Desnormalize pos
            i["lat"] = i["pos"][0]
            i["long"] = i["pos"][1]
            del i["pos"]
            # Desnormalize stops
            for key, value in i["stop"].items():
                i[key] = value
            del i["stop"]
            del i["servicios"]

        with open(f"{json_path}/{k}.json", "w") as f:
            json.dump({
                "horarios": df[k]["horarios"],
                "paraderos": df[k]["paraderos"],
                "trayectos": [{"lat": i[0], "long":i[1]} for i in df[k]["path"]]
            }, f)

    # Load file to bucket
    client = storage.Client.from_service_account_json(KEY_FILE)

    bucket_uris = load_file_to_bucket(client, json_path, BUCKET_NAME, path="daily")
    shutil.rmtree(json_path)

    # Create a BigQuery client and load every csv file in bucket
    client = bigquery.Client.from_service_account_json(KEY_FILE)
    
    # Create dataset if not exists
    check_and_create_dataset(client, DATASET_ID)

    for element in bucket_uris:
        print("ELEMENT:", element, "\n\n\n")

        # Set tables ids
        table_id = ".".join([DATASET_ID, datetime.now().strftime(
            f'%Y%m%d-{element["name"]}')])  # Get current table_id

        # delete table if exists
        if table_exists(client, table_id):
            client.delete_table(table_id)

        # Load the data
        load_file_to_table(client, element, table_id, f="json")

    return "File loaded successfully", 200
