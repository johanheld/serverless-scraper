import json
import time
import pprint
import requests
import boto3
import os
import uuid
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
    "drake's",
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
    "zimmerli",
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


def create_vinted_session() -> requests.Session:
    session = requests.Session()

    # Headers with required anonymous tracking & anti-CSRF values
    headers = {
        **BASE_HEADERS,
        "x-anon-id": str(uuid.uuid4()),
        "x-csrf-token": str(uuid.uuid4()),
    }
    session.headers.update(headers)

    # Initial request to base site to gather required cookies (access_token_web, datadome, etc.)
    response = session.get(BASE_URL)

    # Re-assert authorization cookie if captured
    access_token = session.cookies.get("access_token_web")
    if access_token:
        session.headers["Cookie"] = f"access_token_web={access_token}"

    return session


def scrape_listings():
    session = create_vinted_session()
    listings = []

    for brand in brands:
        brand_listings = fetch_listings(brand, session)
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
    date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
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
        {
            "object_key": s3_object_id,
            "sender_name": sender_name,
            "subject": subject,
            "recipient": recipient,
        }
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


def is_approved_brand(brand: str) -> bool:
    return any(approved_brand.lower() in brand.lower() for approved_brand in brands)


def fetch_listings(brand: str, session: requests.Session) -> list[dict]:
    print(f"Scraping brand: {brand}")
    listings = []

    url = API_URL.format(
        search_text=brand,
        time=int(time.time()),
        session_id=str(uuid.uuid4()),
    )

    print(f"API URL: {url}")
    response = session.get(url)

    # Detailed debugging output
    # print(f"--- [DEBUG] Status Code: {response.status_code} ---")
    # print(f"--- [DEBUG] Response Headers: {response.headers} ---")
    # print(f"--- [DEBUG] Raw Body Preview: {response.text[:1000]} ---")

    if response.status_code != 200:
        print(f"[ERROR] Non-200 HTTP status code returned: {response.status_code}")
        return []

    try:
        data = response.json()
    except ValueError as e:
        print(f"[ERROR] Failed to parse JSON for brand '{brand}': {e}")
        return []

    items = data.get("items", [])
    print(f"[SUCCESS] Items found in payload: {len(items)}")

    for item in items:
        listing = parse_listing(item)

        if not is_approved_brand(listing["brand"]):
            continue

        if not is_valid_listing(listing):
            print("Missing required fields for listing:", listing)
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
    item_box = item.get("item_box", {})
    
    # Extract brand from item_box first_line or top-level fallbacks
    brand = item.get("brand_title") or item_box.get("first_line") or item.get("brand", {}).get("title", "")
    
    # Extract price with fallbacks
    price_obj = item.get("price") or item.get("total_item_price") or {}
    if isinstance(price_obj, dict):
        price = price_obj.get("amount", "")
    else:
        price = str(price_obj)
    
    # Extract size and condition (fallback to item_box second_line e.g., "L · Bra")
    second_line = item_box.get("second_line", "")
    size = item.get("size_title") or (second_line.split("·")[0].strip() if "·" in second_line else second_line) or "N/A"
    condition = item.get("status") or (second_line.split("·")[1].strip() if "·" in second_line else "N/A")

    # Photo URL extraction
    photo = item.get("photo") or {}
    thumbnails = photo.get("thumbnails", [])
    
    img_url = next(
        (
            thumb.get("url")
            for thumb in thumbnails
            if thumb.get("type") in ("thumb310x430", "thumb150x210")
        ),
        photo.get("full_size_url", photo.get("url", "")),
    )

    # Format listing URL
    url = item.get("url", "")
    if url and not url.startswith("http"):
        url = f"https://www.vinted.se{url}"

    return {
        "id": str(item.get("id", "")),
        "brand": brand,
        "price": price,
        "size": size,
        "condition": condition,
        "url": url,
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
    event = {}
    context = {}
    result = lambda_handler(event, context)
    print(result)