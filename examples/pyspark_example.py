from pdf2dataset import download
import shutil
import os
from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel


spark = (
    SparkSession.builder.config("spark.driver.memory", "16G").master("local[16]").appName("spark-stats").getOrCreate()
)

download(
    processes_count=16,
    thread_count=32,
    url_list="s3://my-numina/text5B/part-00080-dc7779ff-a280-46f5-8cd8-bf64283145c8-c000.snappy.parquet",
    output_folder="s3://my-numina/bench",
    output_format="files",
    input_format="parquet",
    url_col="url",
    caption_col="alt",
    enable_wandb=True,
    number_sample_per_shard=1000,
    distributor="pyspark",
    encode_format="pdf",
    retries=3
)

# rm -rf bench
