import os

import pandas as pd

from malevich import collection, pipeline
from malevich.detect import detect
from malevich.models.task import Task
from malevich.utility import download_files, rename_column

storage_endpoint = os.getenv('S3_ENDPOINT')
storage_access_key = os.getenv('S3_ACCESS_KEY')
storage_secret_key = os.getenv('S3_SECRET_KEY')
storage_bucket = os.getenv('S3_BUCKET')


@pipeline(interpreter='core', core_host='http://localhost:8080')
def duolingo(file: str) -> Task:
    files = collection(
        name='files',
        data=pd.DataFrame({
            'filename': [file],
            's3key': [file],
        })
    )

    paths = download_files(files, config={
        'aws_access_key_id': storage_access_key,
        'aws_secret_access_key': storage_secret_key,
        'bucket_name': storage_bucket,
        'endpoint_url': storage_endpoint,
    })

    yolo_inputs = rename_column(paths, config={'filename': 'source'})
    detect(yolo_inputs)


if __name__ == "__main__":
    import argparse

    import boto3

    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, required=True)
    args = parser.parse_args()


    client = boto3.client(
        's3',
        aws_access_key_id=storage_access_key,
        aws_secret_access_key=storage_secret_key,
        endpoint_url=storage_endpoint,
    )

    base_name = os.path.basename(args.file)

    client.upload_file(args.file, storage_bucket, base_name)
    task: Task = duolingo(base_name)

    task.run(with_logs=True, profile_mode='all')
    task.stop()
