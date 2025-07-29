from typing import Tuple

from aws_cdk import (
    Duration,
    Stack,
    aws_ses as ses,
    aws_ssm as ssm,
    aws_sqs as sqs,
    aws_s3 as s3,
    RemovalPolicy,
)
from constructs import Construct
from aws_cdk.aws_lambda import Code, Runtime, LayerVersion
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from aws_cdk.aws_sns import Topic
from aws_cdk.aws_dynamodb import TableV2, Attribute, AttributeType, Billing, Capacity
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from aws_cdk.aws_events import Rule, Schedule
from aws_cdk.aws_events_targets import LambdaFunction


class WebScraperStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.create_second_hand_scrapers()
        # self.create_cph_marathon_scraper()

    def create_second_hand_scrapers(self):
        table_sellpy = self.create_sellpy_table()
        table_vinted = self.create_vinted_table()

        html_bucket = self.create_html_bucket()

        article_topic = self.create_article_topic()

        email_queue = self.create_email_queue()
        sqs_event_source = self.create_sqs_event_source(email_queue)

        chrome_driver_lambda_layer = self.create_chrome_driver_lambda_layer()

        sellpy_scraper_function = self.create_sellpy_scraper_function(
            chrome_driver_lambda_layer,
            article_topic.topic_arn,
            table_sellpy.table_name,
            html_bucket.bucket_name,
            email_queue.queue_url,
        )
        vinted_web_scraper_function = self.create_vinted_web_scraper_function(
            chrome_driver_lambda_layer,
            article_topic.topic_arn,
            table_vinted.table_name,
            html_bucket.bucket_name,
            email_queue.queue_url,
        )
        vinted_api_scraper_function = self.create_vinted_api_scraper_function(
            article_topic.topic_arn,
            table_vinted.table_name,
            html_bucket.bucket_name,
            email_queue.queue_url,
        )
        email_send_function = self.create_email_send_function(html_bucket.bucket_name)

        daily_event_rule = self.create_daily_event_rule()
        daily_event_rule.add_target(LambdaFunction(sellpy_scraper_function))
        daily_event_rule.add_target(LambdaFunction(vinted_api_scraper_function))
        # daily_event_rule.add_target(LambdaFunction(vinted_web_scraper_function))

        email_send_function.add_event_source(sqs_event_source)
        sender_email_param, recipient_email_param = self.get_ssm_params()
        sender_email_identity, recipient_email_identity = self.create_email_identities(
            sender_email_param, recipient_email_param
        )

        self.grant_second_hand_permissions(
            table_sellpy=table_sellpy,
            table_vinted=table_vinted,
            article_topic=article_topic,
            email_queue=email_queue,
            html_bucket=html_bucket,
            sender_email_identity=sender_email_identity,
            recipient_email_identity=recipient_email_identity,
            sender_email_param=sender_email_param,
            recipient_email_param=recipient_email_param,
            sellpy_scraper_function=sellpy_scraper_function,
            vinted_web_scraper_function=vinted_web_scraper_function,
            vinted_api_scraper_function=vinted_api_scraper_function,
            email_send_function=email_send_function,
        )

    def create_cph_marathon_scraper(self):
        ticket_topic = self.create_ticket_topic()
        cph_marathon_scraper_function = self.create_cph_marathon_scraper_function(
            ticket_topic.topic_arn
        )
        ticket_event_rule = self.create_ticket_event_rule()
        ticket_event_rule.add_target(LambdaFunction(cph_marathon_scraper_function))
        ticket_topic.grant_publish(cph_marathon_scraper_function)

    def create_sellpy_table(self) -> TableV2:
        return TableV2(
            self,
            "ArticleTable",
            table_name="articles",
            partition_key=Attribute(name="id", type=AttributeType.STRING),
            billing=Billing.provisioned(
                read_capacity=Capacity.fixed(2),
                write_capacity=Capacity.autoscaled(max_capacity=20, seed_capacity=5),
            ),
        )

    def create_vinted_table(self) -> TableV2:
        return TableV2(
            self,
            "ArticleTableVinted",
            table_name="articles_vinted",
            partition_key=Attribute(name="id", type=AttributeType.STRING),
            billing=Billing.provisioned(
                read_capacity=Capacity.fixed(2),
                write_capacity=Capacity.autoscaled(max_capacity=20, seed_capacity=5),
            ),
        )

    def create_html_bucket(self) -> s3.Bucket:
        return s3.Bucket(
            self,
            "HtmlStorageBucket",
            bucket_name="serverless-scraper-html-emails",
            lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(30))],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

    def create_chrome_driver_lambda_layer(self) -> LayerVersion:
        return LayerVersion(
            self,
            "ChromeDriverLayer",
            compatible_runtimes=[Runtime.PYTHON_3_8],
            code=Code.from_asset(
                "../serverless-scraper/functions/layers/chromedriver/chromedriver.zip"
            ),
        )

    def create_daily_event_rule(self) -> Rule:
        return Rule(
            self,
            "DailyLambdaEvent",
            schedule=Schedule.cron(hour="4", minute="0"),
        )

    def create_ticket_event_rule(self) -> Rule:
        return Rule(
            self,
            "TicketLambdaEvent",
            schedule=Schedule.cron(minute="0/1", hour="0-1,6-23"),
        )

    def create_sellpy_scraper_function(
        self, chrome_driver_layer, topic_arn, table_name, bucket_name, queue_url
    ) -> PythonFunction:
        return PythonFunction(
            self,
            "SellpyScraperFunction",
            function_name="sellpy-scraper",
            runtime=Runtime.PYTHON_3_8,
            handler="lambda_handler",
            entry="./functions/sellpy-scraper",
            layers=[chrome_driver_layer],
            memory_size=1024,
            timeout=Duration.minutes(15),
            environment={
                "SNS_ARN": topic_arn,
                "DYNAMO_TABLE": table_name,
                "S3_HTML_BUCKET": bucket_name,
                "SQS_EMAIL_QUEUE": queue_url,
            },
        )

    def create_vinted_web_scraper_function(
        self, chrome_driver_layer, topic_arn, table_name, bucket_name, queue_url
    ) -> PythonFunction:
        return PythonFunction(
            self,
            "VintedScraperFunction",
            function_name="vinted-web-scraper",
            runtime=Runtime.PYTHON_3_8,
            handler="lambda_handler",
            entry="./functions/vinted-web-scraper",
            layers=[chrome_driver_layer],
            memory_size=1024,
            timeout=Duration.minutes(15),
            environment={
                "SNS_ARN": topic_arn,
                "DYNAMO_TABLE": table_name,
                "S3_HTML_BUCKET": bucket_name,
                "SQS_EMAIL_QUEUE": queue_url,
            },
        )

    def create_vinted_api_scraper_function(
        self, topic_arn, table_name, bucket_name, queue_url
    ) -> PythonFunction:
        return PythonFunction(
            self,
            "VintedApiScraperFunction",
            function_name="vinted-api-scraper",
            runtime=Runtime.PYTHON_3_12,
            handler="lambda_handler",
            entry="./functions/vinted-api-scraper",
            memory_size=1024,
            timeout=Duration.minutes(15),
            environment={
                "SNS_ARN": topic_arn,
                "DYNAMO_TABLE": table_name,
                "S3_HTML_BUCKET": bucket_name,
                "SQS_EMAIL_QUEUE": queue_url,
            },
        )

    def create_email_send_function(self, bucket_name) -> PythonFunction:
        return PythonFunction(
            self,
            "EmailSendFunction",
            function_name="email-send",
            runtime=Runtime.PYTHON_3_12,
            handler="lambda_handler",
            entry="./functions/email-send",
            memory_size=1024,
            timeout=Duration.minutes(1),
            environment={
                "S3_HTML_BUCKET": bucket_name,
            },
        )

    def create_cph_marathon_scraper_function(self, topic_arn) -> PythonFunction:
        return PythonFunction(
            self,
            "CphMarathonFunction",
            function_name="cph-marathon-scraper",
            runtime=Runtime.PYTHON_3_8,
            handler="lambda_handler",
            entry="./functions/cph-marathon-scraper",
            memory_size=512,
            timeout=Duration.minutes(1),
            environment={
                "SNS_ARN": topic_arn,
            },
        )

    def create_sqs_event_source(self, email_queue) -> SqsEventSource:
        return SqsEventSource(email_queue, batch_size=1)

    def create_email_queue(self) -> sqs.Queue:
        return sqs.Queue(
            self,
            "EmailSendQueue",
            queue_name="email-send-queue",
            visibility_timeout=Duration.seconds(60),
        )

    def get_ssm_params(self) -> Tuple[ssm.StringParameter, ssm.StringParameter]:
        sender_email_param = ssm.StringParameter.from_string_parameter_name(
            self, "SenderEmailParameter", string_parameter_name="/ses/email/sender"
        )

        recipient_email_param = ssm.StringParameter.from_string_parameter_name(
            self,
            "RecipientEmailParameter",
            string_parameter_name="/ses/email/recipient",
        )
        return sender_email_param, recipient_email_param

    def create_email_identities(
        self, sender_email_param, recipient_email_param
    ) -> Tuple[ses.EmailIdentity, ses.EmailIdentity]:
        email_sender_identity = ses.EmailIdentity(
            self,
            "EmailSenderIdentity",
            identity=ses.Identity.email(sender_email_param.string_value),
        )

        email_recipient_identity = ses.EmailIdentity(
            self,
            "EmailRecipientIdentity",
            identity=ses.Identity.email(recipient_email_param.string_value),
        )
        return email_sender_identity, email_recipient_identity

    def create_article_topic(self) -> Topic:
        return Topic(self, "ArticleTopic", topic_name="article-topic")

    def create_ticket_topic(self) -> Topic:
        return Topic(self, "TicketTopic", topic_name="ticket-topic")

    def grant_second_hand_permissions(
        self,
        table_sellpy,
        table_vinted,
        article_topic,
        email_queue,
        html_bucket,
        sender_email_identity,
        recipient_email_identity,
        sender_email_param,
        recipient_email_param,
        sellpy_scraper_function,
        vinted_web_scraper_function,
        vinted_api_scraper_function,
        email_send_function,
    ):
        table_sellpy.grant_full_access(sellpy_scraper_function)
        table_vinted.grant_full_access(vinted_web_scraper_function)
        table_vinted.grant_full_access(vinted_api_scraper_function)

        article_topic.grant_publish(sellpy_scraper_function)
        article_topic.grant_publish(vinted_web_scraper_function)
        article_topic.grant_publish(vinted_api_scraper_function)

        sender_email_identity.grant_send_email(vinted_web_scraper_function)
        sender_email_identity.grant_send_email(vinted_api_scraper_function)
        sender_email_identity.grant_send_email(sellpy_scraper_function)
        sender_email_identity.grant_send_email(email_send_function)

        recipient_email_identity.grant_send_email(email_send_function)

        sender_email_param.grant_read(vinted_web_scraper_function)
        sender_email_param.grant_read(vinted_api_scraper_function)
        sender_email_param.grant_read(sellpy_scraper_function)
        sender_email_param.grant_read(email_send_function)

        recipient_email_param.grant_read(vinted_web_scraper_function)
        recipient_email_param.grant_read(vinted_api_scraper_function)
        recipient_email_param.grant_read(sellpy_scraper_function)

        html_bucket.grant_put(vinted_web_scraper_function)
        html_bucket.grant_put(vinted_api_scraper_function)
        html_bucket.grant_put(sellpy_scraper_function)
        html_bucket.grant_read(email_send_function)

        email_queue.grant_send_messages(vinted_web_scraper_function)
        email_queue.grant_send_messages(vinted_api_scraper_function)
        email_queue.grant_send_messages(sellpy_scraper_function)
