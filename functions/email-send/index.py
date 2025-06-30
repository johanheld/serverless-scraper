import json
import boto3
import os

s3 = boto3.client("s3")
ses = boto3.client("ses")
ssm = boto3.client("ssm")

BUCKET_NAME = os.environ["S3_HTML_BUCKET"]
SENDER_EMAIL = ssm.get_parameter(Name="/ses/email/sender")["Parameter"]["Value"]

def lambda_handler(event, context):
    print(event)
    for record in event["Records"]:
        body = json.loads(record["body"])
        
        subject = body["subject"]
        recipient = body["recipient"]
        object_key = body["object_key"]
        
        # Get HTML content from S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=object_key)
        html_content = response["Body"].read().decode("utf-8")
        
        response = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [recipient]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Html": {"Data": html_content}}
            }
        )
        print("Message published to SES:", response["MessageId"])
        
        
    return {"statusCode": 200, "body": "Emails sent successfully"}