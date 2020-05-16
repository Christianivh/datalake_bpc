import boto3
import os
import datetime
from typing import List, Dict

from utils.table_formats import load_json_schema

glue = boto3.client('glue')

GLUE_TABLE_FORMATS = {
    'csv': {
        'Input': 'org.apache.hadoop.mapred.TextInputFormat',
        'Output': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        'Serde': {
            'Lib': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
            'Params': {
                'separatorChar': ','
            }
        },
        'Prefix': '',
        'Key': lambda prefix, table: f"work/{prefix}/{table}",
        'Bucket': os.environ['bucket_output']
    },
    'tsv': {
        'Input': 'org.apache.hadoop.mapred.TextInputFormat',
        'Output': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        'Serde': {
            'Lib': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
            'Params': {
                'field.delim': '\t'
            }
        },
        'Prefix': '',
        'Key': lambda prefix, table: f"work/{prefix}/{table}",
        'Bucket': os.environ['bucket_output']
    }
}

DEFAULT_PARTITION_KEYS = [
    {'Name': 'pt_date', 'Type': 'string'},
    {'Name': 'pt_time', 'Type': 'string'}
]


def table_spec(table_name: str, file_type: str, s3_path: str, columns: List[str], delimiter: Dict[str, dict]):
    """
    Returns a valid table spec for use with Glue CreateTable API
    """

    formats = GLUE_TABLE_FORMATS[file_type]

    return {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Compressed': True,
            'Location': s3_path,
            'InputFormat': formats['Input'],
            'OutputFormat': formats['Output'],
            'SerdeInfo': {
                'SerializationLibrary': formats['Serde']['Lib'],
                'Parameters': formats['Serde']['Params']
            }
        },
        'PartitionKeys': DEFAULT_PARTITION_KEYS,
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE',
            'classification': file_type,
            'creationDate': datetime.datetime.utcnow().isoformat()
        }
    }


def table_exists(database: str, name: str) -> bool:
    """
    Check if given table exists in the Glue catalog
    """
    resp = glue.get_tables(DatabaseName=database, Expression=f".*{name}.*")
    return len(resp['TableList']) > 0


def get_columns(schema_table: str) -> List[Dict[str, str]]:
    columns = [{'Name': field['column_name'], 'Type': 'string'} for field in schema_table]
    return columns


def get_delimiter(table_schema: str) -> Dict[str, dict]:
    delimiter = [field['delimiter'] for field in table_schema][0]
    delimiter = "{'field.delim':'{%s}'}" % delimiter
    delimiter = {'Parameters': eval(delimiter)}
    return delimiter


def create_table(system: str, interface: str, file_type: str, database_name: str):
    """
    Create table for given system and interface, if it exists
    Return name and warehouse path info for the table
    """
    formats = GLUE_TABLE_FORMATS[file_type]
    table_name = f"{formats['Prefix']}{system}_{interface}"
    bucket = formats['Bucket']
    key = formats['Key'](system, table_name)
    s3_path = f"s3://{bucket}/{key}"

    if table_exists(database_name, table_name):
        print('tabla ya existe')
        return table_name, bucket, key

    prefix_schema = 'work/schemas'
    bucket_schema = bucket
    interface_schema = interface

    table_schema = load_json_schema(system, interface_schema, bucket_schema, prefix_schema)

    columns = get_columns(table_schema)
    delimiter = get_delimiter(table_schema)

    table = table_spec(table_name, file_type, s3_path, columns, delimiter)

    try:
        glue.create_table(DatabaseName=database_name, TableInput=table)
    except glue.exceptions.AlreadyExistsException:
        print(f"{table_name} ya existe")

    return table_name, bucket, key


def get_glue_table(database_name: str, table_name: str):
    try:
        response = glue.get_table(DatabaseName=database_name, Name=table_name)

        print(response)
        return response['Table']
    except glue.exceptions.EntityNotFoundException:
        raise ValueError(f'Error:   {database_name}.{table_name} no existe')
        return None


def create_partition(database_name_wrk, table_name, new_partition):
    try:
        print('lm: agregar nueva particion: ', end='')
        response = glue.create_partition(DatabaseName=database_name_wrk,
                                         TableName=table_name,
                                         PartitionInput=new_partition)
        print('Okey particion agregada:')
    except glue.exceptions.AlreadyExistsException as e:
        print('lm:excepcion Particion ya existente', e)


