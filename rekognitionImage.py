import boto3
import os
from decimal import Decimal

reco = boto3.client('rekognition', region_name=os.environ["REGION_REKOGNITION"])

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ["NAME_DYNAMODB_TABLE"])


def analysis_image(bucket, key, data):

    analysis = reco.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        }
    )

    detect_faces = reco.detect_faces(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': key,
            }
        },
        Attributes=[
            'ALL',
        ]
    )

    faces = False
    if detect_faces['FaceDetails']:
        face = detect_faces['FaceDetails'][0]
        faces = {}

        for index, value in face.items():
            if isinstance(value, float):
                faces[index] = Decimal(value)

            if isinstance(value, dict):
                faces[index] = {}
                for nested_index, nested_value in value.items():
                    faces[index][nested_index] = Decimal(nested_value) if isinstance(nested_value, float) else nested_value

            if isinstance(value, list):
                faces[index] = []
                for nested_value in value:
                    faces[index].append(
                        {
                          k:Decimal(v) if isinstance(v, float) else v
                          for k, v in nested_value.items()
                        }
                    )

    table.put_item(
        Item={
            'user_id': int(data[-3]) if data[-3] else 0,
            'uuid': key,
            'type' : data[-4],
            'face': True if faces else False,
            'faces': faces,
            'labels': [{'Confidence': Decimal(b['Confidence']), 'Name': b['Name']} for b in analysis['Labels']]
        }
    )


def handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        size = record['s3']['object']['size']

        if size < 5242880:
            data = key.split('/')
            if len(data) > 3:
                if data[-4] == "pro" or data[-4] == "user" or data[-4] == "member" :
                    analysis_image(bucket, key, data)