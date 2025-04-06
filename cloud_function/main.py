import csv
from google.cloud import storage

def process_csv(event, context):
    bucket = event['bucket']
    name = event['name']
    print(f"Processing file: {name}")

    client = storage.Client()
    blob = client.bucket(bucket).blob(name)
    content = blob.download_as_text()

    for row in csv.DictReader(content.splitlines()):
        print(f"Parsed row: {row}")
        # TODO: send to backend / Google Sheets / further processing
