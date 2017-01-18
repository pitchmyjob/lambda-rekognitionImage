import boto3
from decimal import Decimal

s3_client = boto3.client('s3')

reco = boto3.client('rekognition')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Rekognition')


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
        faces = {
            'Confidence': Decimal(face['Confidence']),
            'Emotions': [{'Confidence': Decimal(b['Confidence']), 'Type': b['Type']} for b in face['Emotions']],
            'Eyeglasses': {
                "Confidence": Decimal(face['Eyeglasses']['Confidence']),
                "Value": face['Eyeglasses']['Value']
            },
            'Sunglasses': {
                "Confidence": Decimal(face['Sunglasses']['Confidence']),
                "Value": face['Sunglasses']['Value']
            },
            "Smile": {
                "Confidence": Decimal(face['Smile']['Confidence']),
                "Value": face['Smile']['Value']
            },
            "Gender": {
                "Confidence": Decimal(face['Gender']['Confidence']),
                "Value": face['Gender']['Value']
            }
        }

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

        data = key.split('/')

        if len(data) > 3:
            if data[-4] == "pro" or data[-4] == "user" or data[-4] == "member" :
                analysis_image(bucket, key, data)