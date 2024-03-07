#!/usr/bin/env python3
import os
import aws_cdk as cdk
from web_scraper.web_scraper_stack import WebScraperStack

app = cdk.App()
WebScraperStack(app, "WebScraperStack")

app.synth()
