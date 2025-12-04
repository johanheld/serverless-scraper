import json
import time
import pprint
import requests
import boto3
import os
from botocore.exceptions import ClientError
from collections import defaultdict
from datetime import datetime, timezone
from dotenv import load_dotenv
from constants import BASE_URL, API_URL, BASE_HEADERS, USER_AGENT

brands = [
    "fedeli",
    "piacenza",
    "fioroni",
    "boglioli",
    "lardini",
    "zanone",
    "incotex",
    "glanshirt",
    "montedoro",
    "sunspel",
    "william lockie",
    "johnstons of elgin",
    "finamore",
    "mazzarelli",
    "altea",
    "aspesi",
    "rubato",
    "etro",
    "loro piana",
    "brunello cucinelli",
    "gran sasso",
    "kiton",
    "ermenegildo",
    "brioni",
    "caruso",
    "satisfy",
    "alden",
    "crockett & jones",
    "mismo",
    "tumi",
]


def lambda_handler(event, context):
    print("-----------handler started------------")

    listings = scrape_listings()
    new_listings = write_to_db(listings)

    if len(new_listings) > 0:
        html = generate_html(new_listings)
        html_s3_object_id = upload_html_to_s3(html)
        push_event_to_sqs(html_s3_object_id, len(new_listings))

    return {"statusCode": 200, "body": json.dumps(len(new_listings))}


def scrape_listings():
    access_token = get_access_token()
    headers = get_api_headers(access_token)

    listings = []

    for brand in brands:
        brand_listings = fetch_listings(brand, headers)
        listings.extend(brand_listings)
        time.sleep(4)

    print(listings)
    print("----------------------")
    print(f"Scraped listings: {len(listings)}")
    return listings


def write_to_db(listings):
    dynamodb = boto3.client("dynamodb")
    new_items = []

    for listing in listings:
        item = {
            "id": {"S": listing["id"]},
            "brand": {"S": listing["brand"]},
            "price": {"S": listing["price"]},
            "size": {"S": listing["size"]},
            "condition": {"S": listing["condition"]},
            "url": {"S": listing["url"]},
            "img_url": {"S": listing["img_url"]},
        }

        condition_expression = "attribute_not_exists(id)"

        try:
            response = dynamodb.put_item(
                TableName=os.environ["DYNAMO_TABLE"],
                Item=item,
                ConditionExpression=condition_expression,
            )

            new_items.append(listing)

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                # The condition expression was not met, indicating that the item already exists
                continue
            else:
                print(e)
                continue

    print("-------------------------")
    print(f"New listings saved: {len(new_items)}")
    print(f"New listings: {new_items}")
    return new_items


def upload_html_to_s3(html):
    bucket_name = os.environ["S3_HTML_BUCKET"]
    date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")  # e.g. "2025-06-28"
    object_key = f"vinted/{date_key}.html"

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name, Key=object_key, Body=html, ContentType="text/html"
    )

    print(f"Uploaded to s3://{bucket_name}/{object_key}")
    return object_key


def push_event_to_sqs(s3_object_id, nbr_of_new_listings):
    sqs = boto3.client("sqs")
    ssm = boto3.client("ssm")

    sender_name = "Vinted"
    subject = f"⚡ {nbr_of_new_listings} new Vinted listings"
    recipient = ssm.get_parameter(Name="/ses/email/recipient")["Parameter"]["Value"]

    message_body = json.dumps(
        {"object_key": s3_object_id, "sender_name": sender_name, "subject": subject, "recipient": recipient}
    )

    response = sqs.send_message(
        QueueUrl=os.environ["SQS_EMAIL_QUEUE"], MessageBody=message_body
    )

    print("-------------------------")
    print("Message published to SQS:", response["MessageId"])


def publish_to_sns(listings):
    client = boto3.client("sns")

    subject = f"{len(listings)} new Vinted listings"
    message = format_message(listings)

    response = client.publish(
        TopicArn=os.environ["SNS_ARN"], Message=message, Subject=subject
    )

    print("-------------------------")
    print("Message published to SNS:", response["MessageId"])


def send_email(listings):
    ses = boto3.client("ses")
    ssm = boto3.client("ssm")

    recipient = ssm.get_parameter(Name="/ses/email/recipient")["Parameter"]["Value"]
    sender = ssm.get_parameter(Name="/ses/email/sender")["Parameter"]["Value"]

    subject = f"{len(listings)} new Vinted listings"
    html = generate_html(listings)

    response = ses.send_email(
        Source=sender,
        Destination={"ToAddresses": [recipient]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Html": {"Data": html}},
        },
    )

    print("-------------------------")
    print("Message published to SES:", response["MessageId"])


def get_access_token() -> str:
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(BASE_URL, headers=headers)

    # print("Cookies received:")
    # for cookie in response.cookies:
    #     print(f"{cookie.name} = {cookie.value}")

    return response.cookies.get("access_token_web")


def get_api_headers(access_token: str) -> dict:
    return {
        **BASE_HEADERS,
        "Cookie": f"access_token_web={access_token}",
    }


def is_approved_brand(brand: str) -> bool:
    return any(approved_brand.lower() in brand.lower() for approved_brand in brands)


def fetch_listings(brand: str, headers: dict) -> list[dict]:
    print(f"Scraping brand: {brand}")
    listings = []

    response = requests.get(API_URL.format(brand), headers=headers)
    items = response.json().get("items", [])

    for item in items:
        listing = parse_listing(item)

        if not is_approved_brand(listing["brand"]):
            # print(f"'{listing['brand']}' does not match any approved brand.")
            continue

        if not is_valid_listing(listing):
            print("Missing fields for listing")
            print(listing)
            continue

        listings.append(listing)

    return listings


def is_valid_listing(listing: dict) -> bool:
    return None not in (
        listing.get("id"),
        listing.get("brand"),
        listing.get("price"),
        listing.get("url"),
        listing.get("img_url"),
    )


def parse_listing(item: dict) -> dict:
    photo = item.get("photo", {})
    thumbnails = photo.get("thumbnails", [])

    # Find thumbnail in specific size
    img_url = next(
        (
            thumb.get("url")
            for thumb in thumbnails
            if thumb.get("type") == "thumb310x430"
        ),
        photo.get("url", ""),  # fallback to original photo url
    )

    return {
        "id": str(item.get("id", "")),
        "brand": item.get("brand_title", ""),
        "price": item.get("total_item_price", {}).get("amount", ""),
        "size": item.get("size_title", "N/A"),
        "condition": item.get("status", "N/A"),
        "url": item.get("url", ""),
        "img_url": img_url,
    }


def format_message(listings):
    listings_by_brand = defaultdict(list)
    for listing in listings:
        listings_by_brand[listing["brand"]].append(listing)

    formatted_data = ""
    for brand, listings in listings_by_brand.items():
        formatted_data += f"{brand}\n\n"
        for listing in listings:
            formatted_data += f"{listing['price']}\n{listing['url']}\n"
        formatted_data += "\n"

    return formatted_data


def generate_html(listings):
    brands = defaultdict(list)
    for item in listings:
        brands[item["brand"]].append(item)

    html = """<!DOCTYPE html>
<html>
<body style="font-family: Arial, sans-serif; font-size: 14px; color: #333; margin:0; padding:0;">
"""

    for brand, items in brands.items():
        html += f"""
  <table width="600" cellpadding="0" cellspacing="0" border="0" align="center" style="border-collapse: collapse; margin-bottom: 20px;">
    <tr>
      <td style="padding: 15px 0 5px 10px;">
        <div style="font-weight: bold; font-size: 28px; padding-bottom: 10px;">
          {brand}
        </div>
      </td>
    </tr>
    <tr><td style="height: 10px;"></td></tr>
"""

        # Rows with two columns per row
        for i in range(0, len(items), 2):
            html += "    <tr>\n"
            for j in range(2):
                if i + j < len(items):
                    item = items[i + j]
                    html += f"""      <td width="50%" valign="top" style="padding: 10px;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="border: 1px solid #ddd; border-collapse: collapse; font-family: Arial, sans-serif; font-size: 14px;">
  <tr>
    <td align="center" style="padding-bottom: 10px;">
      <a href="{item["url"]}" target="_blank">
        <img src="{item["img_url"]}" alt="" style="width: 100%; height: auto; display: block;">
      </a>
    </td>
  </tr>
  <tr>
    <td align="left" style="padding: 10px; font-weight: bold; font-size: 16px; color: #222;">
      {item["brand"]}
    </td>
  </tr>
  <tr>
    <td align="left" style="padding: 0 10px 0px 10px; color: #333;">
      <b>Size:</b> {item["size"]}
    </td>
  </tr>
  <tr>
    <td align="left" style="padding: 0 10px 0px 10px; color: #333;">
      <b>Price:</b> {item["price"]}
    </td>
  </tr>
  <tr>
    <td align="left" style="padding: 0 10px 5px 10px; color: #333;">
     <b>Condition:</b> {item["condition"]}
    </td>
  </tr>
  <tr>
    <td align="left" style="padding: 0 10px 10px 10px; color: #777; font-size: 12px;">
      ID: {item["id"]}
    </td>
  </tr>
</table>
      </td>
"""
                else:
                    html += '      <td width="50%" valign="top" style="padding: 10px;"></td>\n'
            html += "    </tr>\n"

        html += "  </table>\n"

    html += """</body>
</html>
"""

    print("HTML generated.")
    return html


if __name__ == "__main__":
    load_dotenv()
    event = {}  # Provide any necessary event data here
    context = {}  # Provide any necessary context data here
    result = lambda_handler(event, context)
    print(result)
