#!/bin/bash

# Check if at least two arguments are provided
if [ $# -lt 2 ]; then
  echo "Usage: $0 <log-path-s3> <venv-path-s3> <command-arguments...>"
  exit 1
fi

# Assign the first two arguments to variables
LOG_PATH_S3=$1
VENV_PATH_S3=$2
SCRIPT_PATH_S3=$3

# Shift the first two arguments, leaving only the command arguments
shift 2

# Build the Args array as a single compact JSON string
ARGS_JSON=$(jq -c -n --arg venv "$VENV_PATH_S3" --argjson args "$(printf '%s\n' "$@" | jq -R . | jq -s .)" '[
  "spark-submit",
  "--deploy-mode", "cluster",
  "--conf", "spark.yarn.dist.archives=\($venv)#environment",
  "--conf", "spark.pyspark.python=./environment/bin/python",
  "--conf", "spark.pyspark.driver.python=./environment/bin/python",
  "--conf", "spark.driver.memory=16G",
  "--conf", "spark.executor.memory=16G",
  "--conf", "spark.executor.cores=16",
  "--conf", "spark.dynamicAllocation.enabled=true",
  "--conf", "spark.dynamicAllocation.minExecutors=4",
  "--conf", "spark.dynamicAllocation.maxExecutors=64"
] + $args')

# Print the Args JSON for debugging
echo "Args JSON: $ARGS_JSON"

# Create the EMR cluster with dynamic arguments
aws emr create-cluster \
  --name "PDF Download Cluster" \
  --release-label emr-7.6.0 \
  --applications Name=Spark \
  --instance-groups '[
        {"Name":"Master","InstanceGroupType":"MASTER","InstanceType":"c4.4xlarge","InstanceCount":1},
        {"Name":"Core","InstanceGroupType":"CORE","InstanceType":"c4.4xlarge","InstanceCount":1},
        {"Name":"Task","InstanceGroupType":"TASK","InstanceType":"c4.4xlarge","InstanceCount":4}
    ]' \
  --log-uri $LOG_PATH_S3 \
  --ec2-attributes InstanceProfile=pdf2dataset-AmazonEMR-InstanceProfile \
  --service-role pdf2dataset-AmazonEMR-ServiceRole \
  --steps Type=CUSTOM_JAR,Name="pdf2dataset",ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args="$ARGS_JSON" \
  --auto-terminate \
  --region us-east-1
