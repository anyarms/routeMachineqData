import json
import boto3
import botocore
import psycopg2
import os
from datetime import datetime

dbname =  os.environ['dbname']
user = os.environ['user']
host = os.environ['host']
password = os.environ['password']
port = os.environ['port']

connection_string = "port='{}' dbname='{}' user='{}' host='{}' password='{}'"\
    .format(port, dbname, user, host, password)
conn = psycopg2.connect(connection_string)


def findDeviceMeasurementId(s3_location):
    try:
        cur = conn.cursor()
        sql = "SELECT id FROM device_measurements WHERE s3_location=(%s);"
        data = [s3_location]
        cur.execute(sql, data)
        all_found = cur.fetchall()
        cur.execute("commit;")
        cur.close()
        if len(all_found) > 1:
            raise AssertionError('Ambiguous match: more than one record found with provided s3_location')
        elif len(all_found) == 0:
            return ''
        return all_found[0][0]
    except Exception as e:
        print('Error {}'.format(str(e)))
    return ''

def insertRecord(existing_id, json_record, s3_location):
    now = datetime.utcnow()
    response = {}
    if not existing_id:
        response['details'] = 'created record'
        sql = "INSERT INTO device_measurements (s3_location, raw_values, created_at, updated_at) VALUES (%s, %s, %s, %s) RETURNING id"
        data = (s3_location, json_record, now, now)
    else:
        response['details'] = 'updated record'
        sql = "UPDATE device_measurements SET raw_values=(%s) WHERE id=(%s) RETURNING id"
        data = (json_record, existing_id)
    try:
        cur = conn.cursor()
        cur.execute(sql,data)
        response['location'] = cur.fetchone()[0]
        cur.execute("commit;")
        cur.close()
    except Exception as e:
        response['result'] = 'error'
        response['detail'] = 'Error {}'.format(str(e))
    response['result'] = 'success'
    return response

def writeDecodedRecord(parsed_result):
    if 'sensor-records-assembly:' in parsed_result['location']:
        filename = parsed_result['location'].split(':')[1]
        s3 = boto3.client('s3')
        decoded_data = s3.get_object(Bucket='sensor-records-assembly', Key=filename)['Body'].read()
        record_id = findDeviceMeasurementId(parsed_result['location'])
        response = insertRecord(record_id, decoded_data, parsed_result['location'])
    else:
        decoded_data = parsed_result['decoded_value']
        response = insertRecord('', decoded_data, parsed_result['location']) # I never need to update a record, so just write a new one
    conn.close()
    return response
