from aws_cdk import (
    Duration,
    Stack,
    aws_events,
    aws_events_targets
)
from constructs import Construct
from aws_cdk.aws_lambda import Code, Runtime, LayerVersion
from aws_cdk.aws_sns import Topic
from aws_cdk.aws_dynamodb import TableV2, Attribute, AttributeType, Billing, Capacity
from aws_cdk import aws_lambda_python_alpha as lambda_alpha_

class WebScraperStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        table = TableV2(self, "ArticleTable",
            table_name='articles',
            partition_key=Attribute(name="id", type=AttributeType.STRING),
            billing=Billing.provisioned(
                read_capacity=Capacity.fixed(2),
                write_capacity=Capacity.autoscaled(max_capacity=20, seed_capacity=5)
            ),
        )

        table_vinted = TableV2(self, "ArticleTableVinted",
            table_name='articles_vinted',
            partition_key=Attribute(name="id", type=AttributeType.STRING),
            billing=Billing.provisioned(
                read_capacity=Capacity.fixed(2),
                write_capacity=Capacity.autoscaled(max_capacity=20, seed_capacity=5)
            ),
        )

        topic = Topic(self, "ArticleTopic",
            topic_name="article-topic"
        )

        ticket_topic = Topic(self, "TicketTopic",
            topic_name="ticket-topic"
        )

        chrome_driver_layer = LayerVersion(
            self, 'ChromeDriverLayer',
            compatible_runtimes=[Runtime.PYTHON_3_8],
            code=Code.from_asset('../serverless-scraper/functions/layers/chromedriver/chromedriver.zip')
        )
        
        sellpy_scraper_function=lambda_alpha_.PythonFunction(
            self, 'SellpyScraperFunction',
            function_name='sellpy-scraper',
            runtime=Runtime.PYTHON_3_8,
            handler='lambda_handler', 
            entry='./functions/scraper',
            layers=[chrome_driver_layer],
            memory_size=1024,
            timeout=Duration.minutes(15),
            environment={
                'SNS_ARN': topic.topic_arn,
                'DYNAMO_TABLE': table.table_name
            }
        )

        vinted_scraper_function=lambda_alpha_.PythonFunction(
            self, 'VintedScraperFunction',
            function_name='vinted-scraper',
            runtime=Runtime.PYTHON_3_8,
            handler='lambda_handler', 
            entry='./functions/vinted-scraper',
            layers=[chrome_driver_layer],
            memory_size=1024,
            timeout=Duration.minutes(15),
            environment={
                'SNS_ARN': topic.topic_arn,
                'DYNAMO_TABLE': table_vinted.table_name
            }
        )

        cph_half_scraper_function=lambda_alpha_.PythonFunction(
            self, 'CphHalfFunction',
            function_name='cph-half-scraper',
            runtime=Runtime.PYTHON_3_8,
            handler='lambda_handler', 
            entry='./functions/cph-half-scraper',
            memory_size=512,
            timeout=Duration.minutes(1),
            environment={
                'SNS_ARN': ticket_topic.topic_arn,
            }
        )

        rule = aws_events.Rule(
            self, 'DailyLambdaEvent',
            schedule=aws_events.Schedule.cron(hour='4', minute='0'),)
        
        ticket_rule = aws_events.Rule(
            self, 'TicketLambdaEvent',
            schedule=aws_events.Schedule.cron(
                minute='0/1',
                hour='0-1,6-23'
            )
        ) 

        rule.add_target(aws_events_targets.LambdaFunction(sellpy_scraper_function))
        rule.add_target(aws_events_targets.LambdaFunction(vinted_scraper_function))
        ticket_rule.add_target(aws_events_targets.LambdaFunction(cph_half_scraper_function))

        table.grant_full_access(sellpy_scraper_function)
        table_vinted.grant_full_access(vinted_scraper_function)
        topic.grant_publish(sellpy_scraper_function)
        topic.grant_publish(vinted_scraper_function)
        ticket_topic.grant_publish(cph_half_scraper_function)
