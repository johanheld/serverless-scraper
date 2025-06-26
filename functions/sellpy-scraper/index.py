import json
import time
import pprint
import boto3
import os
from bs4 import BeautifulSoup
from selenium import webdriver
from headless_chrome import create_driver
from botocore.exceptions import ClientError
from collections import defaultdict

driver = create_driver()

# For localhost
# driver = webdriver.Chrome()
# os.environ["DYNAMO_TABLE"] = "articles table"
# os.environ["SNS_ARN"] = "sns arn"

brands = [
    "fedeli",
    "zanone",
    "finamore",
    "glanshirt",
    "brunello+cucinelli",
    "sunspel",
    "lardini",
    "alden",
    "crockett+jones",
    "gran+sasso",
    "montedoro",
    "boglioli",
    "brioni",
    "loro+piana",
    "caruso",
    "etro",
    "aspesi",
    "mazzarelli",
    "kiton",
    "mismo",
    "rubato",
    "incotex",
    "zegna",
    "altea",
    "satisfy",
    "tumi",
]


def lambda_handler(event, context):
    print("-----------handler started------------")

    raw_articles = scrape_articles()
    parsed_articles = parse_articles(raw_articles)
    new_articles = write_to_db(parsed_articles)

    if len(new_articles) > 0:
        send_email(new_articles)
        # publish_to_sns(new_articles)
    return {"statusCode": 200, "body": json.dumps(len(new_articles))}


def scrape_articles():
    raw_articles = []
    baseUrl = "https://www.sellpy.se/search?query={}&sortBy=saleStartedAt_desc"

    for brand in brands:
        print(f"Scraping brand: {brand}")
        url = baseUrl.format(brand)
        driver.get(url)

        time.sleep(7)

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        articles = soup.select("article:not(#clipResults-slider article)")

        print(len(articles))
        raw_articles += articles

    print("----------------------")
    print(f"Scraped listings: {len(raw_articles)}")
    return raw_articles


def parse_articles(articles):
    results = []

    for article in articles:
        data = {}

        # Brand
        meta_tag = article.find("meta", itemprop="brand")
        data["brand"] = meta_tag.get("content") if meta_tag else None

        # Title
        item_tag = article.find("p")
        data["title"] = item_tag.text if item_tag else None

        # Price
        price_tag = article.find("p", itemprop="price")
        data["price"] = price_tag.text if price_tag else None

        # Url
        link = article.find("a")
        href = link.get("href") if link else None
        if href is None:
            print("Skipping article - URL not found")
            continue
        data["url"] = "https://www.sellpy.se" + href
        data["id"] = href.split("/")[2]

        # Image
        image_tag = article.find("img")
        data["img_url"] = image_tag.get("src") if image_tag else None

        # Skip article if any required property is missing
        if None in (
            data["brand"],
            data["title"],
            data["price"],
            data["url"],
            data["img_url"],
        ):
            continue

        # If price not set, article is sold
        if "\xa0" in data["price"]:
            continue

        results.append(data)

    # pprint.pp(parsed_articles)
    print("----------------------")
    print(f"Parsed listings: {len(results)}")
    return results


def write_to_db(articles):
    dynamodb = boto3.client("dynamodb")
    new_items = []

    for article in articles:
        item = {
            "id": {"S": article["id"]},
            "brand": {"S": article["brand"]},
            "title": {"S": article["title"]},
            "url": {"S": article["url"]},
            "img_url": {"S": article["img_url"]},
        }

        condition_expression = "attribute_not_exists(id)"

        try:
            response = dynamodb.put_item(
                TableName=os.environ["DYNAMO_TABLE"],
                Item=item,
                ConditionExpression=condition_expression,
            )

            new_items.append(article)

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                # The condition expression was not met, indicating that the item already exists
                continue
            else:
                print(e)
                continue

    print("-------------------------")
    print(f"New listings saved: {len(new_items)}")
    return new_items


def publish_to_sns(articles):
    client = boto3.client("sns")

    subject = f"{len(articles)} new Sellpy listings"
    message = format_message(articles)
    print(message)

    response = client.publish(
        TopicArn=os.environ["SNS_ARN"], Message=message, Subject=subject
    )

    print("-------------------------")
    print("Message published to SNS:", response["MessageId"])


def send_email(articles):
    ses = boto3.client("ses")
    ssm = boto3.client("ssm")

    param = ssm.get_parameter(Name="/ses/email", WithDecryption=False)
    to_address = param["Parameter"]["Value"]

    subject = f"{len(articles)} new Sellpy listings"
    html = generate_html(articles)

    response = ses.send_email(
        Source=to_address,
        Destination={"ToAddresses": [to_address]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Html": {"Data": html}},
        },
    )

    print("-------------------------")
    print("Message published to SES:", response["MessageId"])


def format_message(articles):
    articles_by_brand = defaultdict(list)
    for article in articles:
        articles_by_brand[article["brand"]].append(article)

    formatted_data = ""
    for brand, articles in articles_by_brand.items():
        formatted_data += f"{brand}\n\n"
        for article in articles:
            formatted_data += (
                f"{article['title']} - {article['price']}\n{article['url']}\n"
            )
        formatted_data += "\n"

    return formatted_data


def generate_html(articles):
    brands = defaultdict(list)
    for item in articles:
        brands[item["brand"]].append(item)

    html = """<!DOCTYPE html>
<html>
<body style="font-family: Arial, sans-serif; font-size: 14px; color: #333; margin:0; padding:0;">
"""

    for brand, items in brands.items():
        html += f"""
  <table width="600" cellpadding="0" cellspacing="0" border="0" align="center" style="border-collapse: collapse; margin-bottom: 20px;">
    <tr>
      <td style="padding: 15px 0 5px 0;">
        <div style="font-weight: bold; font-size: 22px; padding-bottom: 10px;">
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
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border: 1px solid #ddd; border-collapse: collapse;">
          <tr>
            <td align="center" style="padding-bottom: 10px;">
                <a href="{item['url']}" target="_blank">
                    <img src="{item['img_url']}" alt="" style="width: 100%; height: auto; display: block;">
                </a>
            </td>
          </tr>
          <tr>
            <td align="center" style="padding-bottom: 5px; font-weight: bold;">
              {item["title"]} - Size {item["price"]}
            </td>
          </tr>
          <tr>
            <td align="center" style="color: #555;">
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
    event = {}  # Provide any necessary event data here
    context = {}  # Provide any necessary context data here
    result = lambda_handler(event, context)
    print(result)
