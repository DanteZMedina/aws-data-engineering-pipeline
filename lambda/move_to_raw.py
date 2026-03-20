import json
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # 🚨 evitar loop
        if key.startswith("processed/") or key.startswith("raw/"):
            continue

        filename = key.split("/")[-1]

        # mover a raw/
        new_key = f"raw/{filename}"

        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=new_key
        )

        s3.delete_object(Bucket=bucket, Key=key)

        print(f"Archivo movido a raw/: {new_key}")

    return {
        'statusCode': 200,
        'body': json.dumps('Archivo procesado')
    }
