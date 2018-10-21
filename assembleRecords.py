import json
import boto3
import sys
from datetime import datetime, timedelta, timezone

def parse_timestamp(timestamp_string):
    return datetime.strptime(timestamp_string, '%Y-%m-%dT%H:%M:%S.%fZ')

def create_time_record(record):
    s3 = boto3.client('s3')
    filename = record['deveui'] + '/measured_at/' + record['filepath_date'] + '/' + record['value'] + '.json'
    response = { 'location': 'sensor-records-assembly:' + filename,
        'decoded_value': record['value']}
    existing = s3.list_objects_v2(Bucket='sensor-records-assembly', Prefix=filename)
    if existing['KeyCount'] == 0:
        s3.put_object(Bucket='sensor-records-assembly', Body=str(json.dumps({'measured_at': record['value']})), Key=filename)
        response['result'] = 'success'
        response['details'] = 'time record created'
    else:
        response['result'] = 'no_action'
        response['details'] = 'time record already present'
    return response
def append_measurement_record(record):
    s3 = boto3.client('s3')
    response = { 'decoded_value': record['value'] }
    this_month = record['deveui'] + '/created_at/' + record['filepath_date'] + '/01/' + record['set']
    record_set = s3.list_objects_v2(Bucket='sensor-records-assembly', Prefix=this_month)
    possible_timestamps = []
    if 'Contents' in s3.list_objects_v2(Bucket='sensor-records-assembly', Prefix=this_month):
        possible_timestamps = ['Contents']
    qualified_timestamps = []
    for possible in possible_timestamps:
        this_time = parse_timestamp(possible['Key'].split('/')[-1].split('Z')[0] + 'Z')
        #if timestamp in key is earlier than created_at, but later than 2550 minutes earlier
        if  this_time < record['created_at_time'] and this_time > (record['created_at_time'] - timedelta(minutes = (255 * 6))):
            qualified_timestamps.append(possible['Key'])
    if len(qualified_timestamps) == 0: # error, no records found
        response['result'] = 'error'
        response['location'] = ''
        response['details'] = 'no matching time record found for measurement'
    else: # ok, append record
        timestamp = qualified_timestamps[0][-15:-5]
        time = datetime.utcfromtimestamp(int(timestamp))
        filename = record['deveui'] + '/measured_at/' + datetime.strftime(time, '%Y/%m') + '/' + timestamp + '.json'
        response['location'] = 'sensor-records-assembly:' + filename
        s3_response = s3.get_object(Bucket='sensor-records-assembly', Key=filename)
        body = json.loads(s3_response['Body'].read())
        if record['type_string'] in body.keys(): # duplicate or conflict
            if body[record['type_string']] == record['value']: # duplicate
                response['result']= 'no_action'
                response['details']= 'time record found, identical data already entered'
            else: # conflict
                response['result'] = 'error'
                response['details'] = 'time record found, conflicting data already entered'
        else:
            body[record['type_string']] = record['value']
            s3.put_object(Bucket='sensor-records-assembly', Key=filename, Body=str(json.dumps(body)))
            response['result'] = 'success'
            response['details'] = 'measurement appended to time record'
    return response

def parse_record(event):
    type = event['FPort']
    hex = event['payload_hex']
    time = parse_timestamp(event['Time'])
    if type == '1':
        # val = str(int(hex[2:], 16))
        val = str(time.replace(tzinfo=timezone.utc).timestamp()).split('.')[0]
        type_string = 'measured_at'
        type = '01'
    elif type in ['21', '23', '24', '27', '28']:
        val = str(bytearray.fromhex(hex[2:]).decode())
        types = {'21': 'battery', '23': 'distance', '24': 'velocity_frequency', '27': 'temperature', '28': 'is_wet'}
        type_string = types[type]
    else:
        type_string = 'unknown'
        val = hex[2:]

    return {
        'set': str(int(hex[:2], 16)),
        'value': val,
        'type': type,
        'type_string': type_string,
        'created_at': event['Time'],
        'created_at_time': time,
        'filepath_date': datetime.strftime(time, '%Y/%m'),
        'deveui': event['DevEUI']
        }

def handle_assembly(event):
    s3 = boto3.client('s3')
    record = parse_record(event)
    base_path = record['deveui'] + '/created_at/' + record['filepath_date'] + '/'
    filename = base_path + record['type'] + '/' + record['set'] + '/' + record['created_at'] + '.' + record['value'] + '.json'
    error_filename = 'errors/' + filename
    if record['type'] == '01':
        response = create_time_record(record)
    elif record['type'] in  ['21', '23', '24', '27', '28']:
        response = append_measurement_record(record)
    record.update(response)
    event['parsed_data'] = record
    if response['result'] == 'error':
        s3.put_object(Bucket='sensor-records-assembly', Body=str(event), Key=error_filename)
    return response
