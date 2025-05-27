import boto3
import os
import requests
from bs4 import BeautifulSoup

# TICKET_URL_MARATHON = 'https://secure.onreg.com/onreg2/bibexchange/?eventid=6591&language=us'
TICKET_URL_HALF_MARATHON = 'SET NEW URL'
SNS_ARN = os.environ['SNS_ARN']

def lambda_handler(event, context):
    print('-----------handler started------------')
    
    available_ticket, status = check_tickets()
    
    if available_ticket:
        publish_to_sns('AVAILABLE')
    
    return {
        'statusCode': 200,
        'body': f'tickets available: {available_ticket}'
    }

def check_tickets():
    response = requests.get(TICKET_URL_HALF_MARATHON)
    soup = BeautifulSoup(response.text, 'html.parser')

    # For local test files
    # with open('sample-pending.html', 'r', encoding='utf-8') as file:
    #     html_content = file.read()

    # soup = BeautifulSoup(html_content, 'html.parser')

    page_text = soup.get_text()

    if "There are currently no race numbers for sale" in page_text:
        print("No tickets available")
        return False, False 
    if "In progress" in page_text:
        print("Ticket in progress")
        return False, True

    print("Text not found the page. Tickets available!")
    print(page_text)
    return True, True

def publish_to_sns(status):
    client = boto3.client('sns')
    
    subject = f'TICKETS {status}'
    message = f'Tickets are {status.lower()}.\nCheck the link: {TICKET_URL_HALF_MARATHON}'

    response = client.publish(
        TopicArn= SNS_ARN,
        Message=message,
        Subject=subject
    )

    print('-------------------------')
    print("Message published to SNS:", response['MessageId'])
            
if __name__ == '__main__':
    event = {}  # Provide any necessary event data here
    context = {}  # Provide any necessary context data here
    result = lambda_handler(event, context)
    print(result)