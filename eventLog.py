import json
import boto3
from datetime import datetime, timedelta, timezone

def parse_timestamp(timestamp_string):
    return datetime.strptime(timestamp_string, '%Y-%m-%dT%H:%M:%S.%fZ')

def write_to_log(event):
    s3 = boto3.client('s3')
    string_time = event['Time']
    time = parse_timestamp(string_time)
    time_path = datetime.strftime(time, '%Y/%m') + '/' + string_time + '.json'
    deveui_location = event['DevEUI']+'/'+ time_path
    all_location = 'all/' + time_path
    s3.put_object(Bucket='sensor-records-event-log', Body=str(event), Key=deveui_location)
    s3.put_object(Bucket='sensor-records-event-log', Body=str(event), Key=all_location)
    return { 'written_to': [deveui_location, all_location] }
