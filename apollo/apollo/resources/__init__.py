from dagster_databricks import databricks_client
from dagster_aws.s3 import S3Resource
from dagster import EnvVar


databricks_client_instance = databricks_client.configured(
    {
        "host": {"env": EnvVar("DATABRICKS_HOST")},
        "token": {"env": EnvVar("DATABRICKS_TOKEN")},
    }
)
s3_resource = S3Resource(
    region_name="us-gov-west-1",
    aws_access_key_id=EnvVar('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=EnvVar('AWS_SECRET_ACCESS_KEY'),
)

