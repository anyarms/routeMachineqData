import json

from eventLog import write_to_log
from assembleRecords import handle_assembly
from decodeRecords import decodeBitmap
from writetoRDS import writeDecodedRecord

def handler(event, context):
    # Lambda test events come in a dict, but API GW events come in as a str
    if isinstance(event, dict):
        event_body = event['body']
    else:
        event_body = json.loads(event['body'])
    print({ 'event_body': event_body })


    record_parsing_result = {   'result': 'no_action',
                                'location': '',
                                'details': 'No parsing attempted' }
    write_to_rds_result = {     'result': 'no_action',
                                'location': '',
                                'details': 'RDS Write not attempted' }
    if event_body['FPort'] in ['1', '21', '23', '24', '27', '28']:
        record_parsing_result = handle_assembly(event_body)
    elif event_body['FPort'] in ['5']:
        record_parsing_result = decodeBitmap(event_body)
    print({'record_parse': record_parsing_result})
    if record_parsing_result['result'] == 'success':
        write_to_rds_result = writeDecodedRecord(record_parsing_result)
    print({'write_to_rds': write_to_rds_result})

    event_body['processing_details'] = {
        "record_parse": record_parsing_result,
        "write_to_rds": write_to_rds_result }
    event_log_result = write_to_log(event_body)
    print({ 'event_log': event_log_result })

    return {
        "statusCode": 200,
        "body": json.dumps(
            { "event_log": event_log_result,
              "record_parse": record_parsing_result,
              "write_to_rds": write_to_rds_result }
        )
    }
