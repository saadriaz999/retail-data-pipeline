import aws_cdk as cdk
from retail_stack import RetailPipelineStack

app = cdk.App()
RetailPipelineStack(app, "RetailPipelineStack")
app.synth()
