import json
import boto3
import datetime
import os
from utils import glueutils

s3 = boto3.client('s3')


def lambda_handler(event, context):
    print(f'S3 Evento:  {event}')

    if isinstance(event, str):
        event = json.loads(event)

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print(f'Bucket: {bucket}, Key: {key}')

    now = datetime.datetime.utcnow()

    pt_date = f'{now.year}_{now.month}_{now.day}'
    pt_time = f'{now.hour}_{now.minute}_{now.second}'

    print("Particiones Fecha: ", pt_date, " Hora:", pt_time)

    try:
        file = s3.get_object(Bucket=bucket, Key=key)
        file = file['Body'].read()
        filebody = file.decode('utf-8', errors='replace')

        line_count = filebody.count('\n')
        print(f'El archivo que esta llegando contiene {line_count} registros')

        interface = key.split('/')[1]
        system = interface
        key_output = os.environ['prefix_output']
        database_name_wrk = os.environ['database_name_wrk']
        file_type = 'csv'

        print('lm: creando tabla en Glue: ', end="")
        table_name, table_bucket, table_key = glueutils.create_table(system=system, interface=interface,
                                                                     file_type=file_type,
                                                                     database_name=database_name_wrk)
        print('Termino OKs')

        key_output = f'{table_key}/pt_date={pt_date}/pt_time={pt_time}/{interface}.csv'

        print('lm: subiendo archivo al nuevo repositorio', end='')
        s3.put_object(Bucket=os.environ['bucket_output'], Key=key_output, Body=filebody.encode('utf-8'))
        print('Termino OKs')

        print('lm: traer la tabla de glue: ', end="")
        table = glueutils.get_glue_table(database_name_wrk, table_name)
        print('ok')

        partition = f"pt_date={pt_date}/pt_time={pt_time}"

        print('lm: configurar nueva particion')
        new_partition = {
            'Values': [pt_date, pt_time],
            'StorageDescriptor': {
                'OutputFormat': table['StorageDescriptor']['OutputFormat'],
                'InputFormat': table['StorageDescriptor']['InputFormat'],
                'SerdeInfo': table['StorageDescriptor']['SerdeInfo'],
                'Columns': table['StorageDescriptor']['Columns'],
                'Location': f"s3://{table_bucket}/{table_key}/{partition}"
            }
        }

        print('lm: agregar nueva particion: ', end='')
        response = glueutils.create_partition(database_name_wrk=database_name_wrk,
                                              table_name=table['Name'],
                                              new_partition=new_partition)
        print('Okey particion agregada:', partition)

    except Exception as ex:
        print('Ups: ', ex)

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
