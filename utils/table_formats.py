import json
import boto3


def load_json_schema(system: str, interface: str, bucket, prefix):
    """
    Load a FMT file from S3.
    """

    s3 = boto3.resource('s3')
    system = system.lower()
    key = f"{prefix}/{system}/{interface}.json"
    print(f"Loading FMT s3://{bucket}/{key}")
    filejson = s3.Object(bucket, key)
    mdata = filejson.get()['Body'].read().decode()
    schema_table = json.loads(mdata)

    return schema_table
