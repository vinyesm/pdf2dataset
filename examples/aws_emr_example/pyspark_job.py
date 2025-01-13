import fire
from pdf2dataset import download
from pyspark.sql import SparkSession


if __name__ == "__main__":
    print("Initializing Spark session...")
    spark = SparkSession.builder.getOrCreate()
    print("Spark session initialized...")
    fire.Fire(download)