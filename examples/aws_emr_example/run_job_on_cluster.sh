#!/bin/bash

# Check if all required arguments are provided
if [ $# -lt 5 ]; then
  echo "Usage: $0 <log-path-s3> <venv-path-s3> <script-path-s3> <input-url-path-s3> <output-folder-s3>"
  exit 1
fi

# Assign arguments to variables
LOG_PATH_S3=$1
VENV_PATH_S3=$2
SCRIPT_PATH_S3=$3
INPUT_URL_PATH_S3=$4
OUTPUT_FOLDER_S3=$5

# Create the EMR cluster with dynamic arguments
aws emr create-cluster \
  --name "Simple PDF Download Cluster" \
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
  --steps Type=CUSTOM_JAR,Name="pdf2dataset",ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=["spark-submit","--deploy-mode","cluster","--conf","spark.yarn.dist.archives=$VENV_PATH_S3#environment","--conf","spark.pyspark.python=./environment/bin/python","--conf","spark.pyspark.driver.python=./environment/bin/python","--conf","spark.driver.memory=16G","--conf","spark.executor.memory=16G","--conf","spark.executor.cores=16","--conf","spark.dynamicAllocation.enabled=true","--conf","spark.dynamicAllocation.minExecutors=4","--conf","spark.dynamicAllocation.maxExecutors=64","$SCRIPT_PATH_S3","--enable_wandb=False","--url_list=$INPUT_URL_PATH_S3","--output_folder=$OUTPUT_FOLDER_S3"] \
  --auto-terminate \
  --region us-east-1
