import struct
import json
from collections import namedtuple

Record = namedtuple('Record','WakeNumber Time BatTemp BatVolt Depth Vel Temp IsWet Reset Status Bounds') #return data struct

def dataFrameDecode(dataFrame):
   dataBytes = bytes(bytearray.fromhex(dataFrame)) #hex to byte array conversion
   #H unsigned short, I unsigned int, h signed short, c char, x pad byte, B unsigned char, >big-endian
   myRec = Record._make(struct.unpack('>HIhHHhhcxxxxxxBBB',dataBytes)) #only line that should have to change
   #if it changes will most likely only be the > symbol to <
   myRec = myRec._replace(BatTemp = (myRec.BatTemp / 100.0))
   myRec = myRec._replace(BatVolt = (myRec.BatVolt / 100.0))
   myRec = myRec =  myRec._replace(Temp = (myRec.Temp / 100.0))
   return myRec

def record_to_json(record):
    return json.dumps({
        'wake_number': record[0],
        'measured_at': record[1],
        'battery_temperature': record[2],
        'battery_voltage': record[3],
        'distance': record[4],
        'velocity_frequency': record[5],
        'battery_temperature': record[6],
        'is_wet': str(record[7]),   # TODO Fix this, are "b'\\x00'" values truthy or falsy?
        'reset': record[8],
        'status': record[9],
        'bounds': record[10]
    })

def decodeBitmap(event):
    record = dataFrameDecode(event) # (event['payload_hex'])
    return {
        'result': 'success',
        'location': '',
        'details': 'Record Parsed',
        'decoded_value': record_to_json(record)
        }

# dataFrames =["00005BBD6D8408A2053C04C4F22A04BA54000000000000083F00",
#     "00015BBD6FDC08B0053204BAF35804CE54000000000000003F00",
#     "00025BBD71800870052804ECF12804EC54000000000002203F00",
#     "11A95BBD6D84074404A605DC0000000000000000000003000300",
#     "11AA5BBD6FDC06C204BA00000000000000000000000004080100",
#     "11AB5BBD7234FF9C041A000030F3007854000000000005003D00",
#     "11AC5BBD748C08CA05281388729F05F054000000000006003F0A",
#     "FFFF5BBD6D840712052805DC0064064A46000000000007003F00",
#     "00005BBD6FDC0726051E05DC0064065446000000000008003F00"]
#
#
# for x in dataFrames:
#    print(decodeBitmap(x))
