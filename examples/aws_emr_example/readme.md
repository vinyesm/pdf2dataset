## Instructions for launching the pdf download on EMR AWS cluster


### 1. Upload ressources to s3

* upload url list (parquet files) to `s3://<your-bucket>/url_list/*.parquet`

* package and upload the environment to s3

Package your environment using virtualenv

```bash
python -m venv pyspark_venv
source pyspark_venv/bin/activate
pip install pyarrow pandas venv-pack
venv-pack -o pyspark_venv.tar.gz
```

and upload to `s3://<your-bucket>/pyspark_venv.tar.gz`

* upload your script to s3://<your-bucket>/pyspark_job.py

### 2. Create Service and Instance Roles with necessary policies

```bash
aws iam create-role \
  --role-name pdf2dataset-AmazonEMR-InstanceProfile \
  --assume-role-policy-document file://emr-instance-profile-trust.json
aws iam attach-role-policy \
  --role-name pdf2dataset-AmazonEMR-InstanceProfile \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam create-instance-profile \
  --instance-profile-name pdf2dataset-AmazonEMR-InstanceProfile
aws iam add-role-to-instance-profile \
  --instance-profile-name pdf2dataset-AmazonEMR-InstanceProfile \
  --role-name pdf2dataset-AmazonEMR-InstanceProfile

aws iam create-role \
  --role-name pdf2dataset-AmazonEMR-ServiceRole \
  --assume-role-policy-document file://emr-service-role-trust.json
aws iam attach-role-policy \
  --role-name pdf2dataset-AmazonEMR-ServiceRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole
aws iam attach-role-policy \
  --role-name pdf2dataset-AmazonEMR-ServiceRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

### 3. Create the cluster and run the job

`bash run_job_on_cluster.sh s3://my-numina/logs s3://my-numina/env5.tar.gz s3://my-numina/pyspark_job.py s3://my-numina/CC-text5B-math/math-url-sample-00000.parquet s3://my-numina/bench-math-pdf-small`

`bash run_job_on_cluster.sh s3://my-numina/logs s3://my-numina/env5.tar.gz s3://my-numina/pyspark_job.py s3://my-numina/CC-text5B-math s3://my-numina/bench-math-pdf-big`