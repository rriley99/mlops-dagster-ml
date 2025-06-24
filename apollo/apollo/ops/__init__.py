import io
import pandas
from dagster_databricks import databricks_client, databricks_solid
from dagster import op, AssetMaterialization, AssetExecutionContext
#from dagster_aws.s3 import S3Resource
from dagster_aws.s3.resources import s3_resource

# @databricks_
# def trigger_custom_databricks_job(context, job_name: str, parameters: dict) -> str:
#     # Connect to Databricks using the configured parameters
#     # databricks_client = databricks_client.configured(
#     #     {
#     #         "host": {"env": "DATABRICKS_HOST"},
#     #         "token": {"env": "DATABRICKS_TOKEN"},
#     #     }
#     # )
#     databricks_client.connect(host=context.resources.databricks.host,
#                               token=context.resources.databricks.token)
#     # Trigger the custom Databricks job using the Jobs API
#     job_run_id = databricks_client.submit_job_run(job_name, parameters)
#     return job_run_id


# @op(
#     required_resource_keys={'s3'}
# )
# def read_s3_and_materialize_as_asset(s3_bucket: str, s3_key: str) -> str:
#     # Read data from S3
#     s3_client = s3_resource
#     response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
#     data = response['Body'].read().decode('utf-8')
#
#     # Materialize the data as an asset
#     yield AssetMaterialization(asset_key="s3_input_data", description="Input data from S3")
#
#     return data
#
#
# @op(
#     required_resource_keys={'s3'}
# )
# def write_to_s3_and_materialize_as_asset(data: pandas.DataFrame, s3_bucket: str, s3_key: str) -> str:
#     # Write data to CSV
#     data.to_csv(f'data/{s3_key}', index=False)
#     # Write data to S3
#     s3_client = s3_resource
#     s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=f'data/{s3_key}')
#
#     # Materialize the S3 path as an asset
#     # yield AssetMaterialization(asset_key="s3_output_data", description="Output data written to S3")
#
#     return f"s3://{s3_bucket}/{s3_key}"
