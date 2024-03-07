# Serverless web scaper for AWS Lambda using Selenium

This is a serverless web scraper built for AWS lambda running Chrome in headless mode

The script is used for subscribing to new Sellpy listings

Every morning the script will run and find new listings added since the previous run from followed brands. New listings are published to an SNS topic

The project is automated and made using only AWS free tier components

The code is written in Python and CDK is used to deploy the application to AWS

## Infrastructure

-CDK for deploys
-Lambda to run scraper
-Lambda layers to host Chromedriver and Headless Chrome binaries
-DynamoDb to keep track of which listings are new
-EventBridge to automate lambda invoke
-SNS to send out emails with new listings