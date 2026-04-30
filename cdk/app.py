#!/usr/bin/env python3
import os

import aws_cdk as cdk

from cdk.globalpartners_stack import GlobalPartnersStack


app = cdk.App()
GlobalPartnersStack(
    app, 
    "GlobalPartnersStack",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)

app.synth()
