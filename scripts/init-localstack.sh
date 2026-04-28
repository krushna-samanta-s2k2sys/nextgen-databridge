#!/bin/bash
echo "Initializing LocalStack S3 buckets..."
awslocal s3 mb s3://nextgen-databridge-pipeline-configs
awslocal s3 mb s3://nextgen-databridge-duckdb-store
awslocal s3 mb s3://nextgen-databridge-artifacts
awslocal sqs create-queue --queue-name nextgen-databridge-alerts
awslocal sqs create-queue --queue-name nextgen-databridge-deployments
echo "LocalStack initialization complete"
