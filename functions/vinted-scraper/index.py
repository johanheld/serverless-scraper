import json
import time
import pprint
import boto3
import os
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from headless_chrome import create_driver
from botocore.exceptions import ClientError
from collections import defaultdict

brands = ['fedeli', 'zanone',
          'finamore', 'glanshirt',
          'brunello+cucinelli',
          'sunspel', 'lardini',
          'alden', 'crockett+jones',
          'gran+sasso', 'montedoro',
          'boglioli', 'brioni',
          'loro+piana', 'caruso', 'etro',
          'aspesi', 'mazzarelli', 'kiton',
          'mismo'
         ]

def lambda_handler(event, context):
    print('-----------handler started------------')
    
    raw_articles = scrape_articles()
    parsed_articles = parse_articles(raw_articles)
    new_articles = write_to_db(parsed_articles)
    if len(new_articles) > 0:
        publish_to_sns(new_articles)
    return {
        'statusCode': 200,
        'body': json.dumps(len(new_articles))
    }

def scrape_articles():
    raw_articles = []
    driver = create_driver()
    # driver = webdriver.Chrome()
    baseUrl = 'https://www.vinted.se/catalog?search_text={}&order=newest_first&catalog[]=5&page=1'
    
    for brand in brands:
        print(f"Scraping brand: {brand}")
        url = baseUrl.format(brand) 
        driver.get(url)

        time.sleep(3)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        articles = soup.find_all('div', {'data-testid': 'grid-item'})

        print(len(articles))
        raw_articles += articles
    
    print('----------------------')
    print(f"Scraped listings: {len(raw_articles)}")   
    return raw_articles

def parse_articles(articles):
    results = []
    
    for article in articles:
        data = {}
        
        # Extract brand and assert its correct
        brand_tag = article.find('p', class_='web_ui__Text__text', attrs={'data-testid': lambda x: x and 'description-title' in x})
        brand = brand_tag.text if brand_tag else 'Brand not found'
        
        # Check if the brand matches or contains any approved brand (case-insensitive)
        if any(approved_brand.lower() in brand.lower() for approved_brand in brands):
            print(f"'{brand}' matches an approved brand.")
        else:
            print(f"'{brand}' does not match any approved brand.")
            continue

        data['brand'] = brand

        
        # Extract size
        size_tag = article.find('p', class_='web_ui__Text__text', attrs={'data-testid': lambda x: x and 'description-subtitle' in x})
        data['size'] = size_tag.text if size_tag else 'Size not found'

        # Extract price with fee
        price_with_fee_tag = article.find('p', class_='web_ui__Text__text', attrs={'data-testid': lambda x: x and 'price-text' in x})
        data['price'] = price_with_fee_tag.find_next('p').text if price_with_fee_tag else 'Price with fee not found'
        
        # Url
        link = article.find('a', class_='new-item-box__overlay')
        href = link.get('href') if link else None
        if href is None:
            print("Skipping article - URL not found")
            continue
        data['url'] = href
        match = re.search(r'/items/(\d+)-', href)#   href.split('/')[4]
        
        if match:
            data['id'] = match.group(1)  # Extract the number part
        else:
            print("No item number found in the URL")
            
        # Img url
        img_div = article.find('div', class_='web_ui__Image__portrait')
        img_tag = img_div.find('img')
        if img_tag and 'src' in img_tag.attrs:
            data['img_url'] = img_tag['src']
        else:
            print("No image tag found inside the div.")
        
        # Skip article if any required property is missing
        if None in (data['id'], data['brand'], data['price'], data['url'], data['img_url']):
            continue

        results.append(data)
        
    # pprint.pp(results)
    print('----------------------')
    print(f"Parsed listings: {len(results)}")   
    return results

def write_to_db(articles):
    dynamodb = boto3.client('dynamodb')
    new_items = []

    for article in articles:
        item = {
            'id': {'S': article['id']},
            'brand': {'S': article['brand']},
            'price': {'S': article['price']},
            'size': {'S': article['size']},
            'url': {'S': article['url']},
            'img_url': {'S': article['img_url']},
        }

        condition_expression = 'attribute_not_exists(id)'

        try:
            response = dynamodb.put_item(
                TableName=os.environ['DYNAMO_TABLE'],
                Item=item,
                ConditionExpression=condition_expression
            )

            new_items.append(article)

        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                # The condition expression was not met, indicating that the item already exists
                continue
            else:
                print(e)
                continue
    
    print('-------------------------')
    print(f"New listings saved: {len(new_items)}")    
    return new_items

def publish_to_sns(articles):
    client = boto3.client('sns')
    
    subject = f'{len(articles)} new Vinted listings'
    message = format_message(articles)

    response = client.publish(
        TopicArn= os.environ['SNS_ARN'],
        Message=message,
        Subject=subject
    )

    print('-------------------------')
    print("Message published to SNS:", response['MessageId'])

def format_message(articles):
    articles_by_brand = defaultdict(list)
    for article in articles:
        articles_by_brand[article['brand']].append(article)

    formatted_data = ""
    for brand, articles in articles_by_brand.items():
        formatted_data += f"{brand}\n\n"
        for article in articles:
            formatted_data += f"{article['price']}\n{article['url']}\n"
        formatted_data += "\n"

    return formatted_data
            
if __name__ == '__main__':
    event = {}  # Provide any necessary event data here
    context = {}  # Provide any necessary context data here
    result = lambda_handler(event, context)
    print(result)