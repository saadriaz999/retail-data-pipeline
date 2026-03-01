#!/usr/bin/env python3
import os
import aws_cdk as cdk
from analytics.analytics_stack import AnalyticsStack

app = cdk.App()
AnalyticsStack(
    app, "RetailAnalyticsStack",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)
app.synth()