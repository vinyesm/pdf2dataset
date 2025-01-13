import fire
from pdf2dataset import download
from pyspark.sql import SparkSession

def main(
    processes_count=16,
    thread_count=32,
    url_list="s3://my-numina/CC-text5B-math",
    output_folder="s3://my-numina/bench-math",
    output_format="files",
    input_format="parquet",
    url_col="url",
    caption_col="alt",
    enable_wandb=True,
    number_sample_per_shard=1000,
    distributor="pyspark",
    encode_format="pdf",
    retries=3,
):
    # Initialize Spark session
    spark = SparkSession.builder.getOrCreate()

    # Run the download function with provided arguments
    download(
        processes_count=processes_count,
        thread_count=thread_count,
        url_list=url_list,
        output_folder=output_folder,
        output_format=output_format,
        input_format=input_format,
        url_col=url_col,
        caption_col=caption_col,
        enable_wandb=enable_wandb,
        number_sample_per_shard=number_sample_per_shard,
        distributor=distributor,
        encode_format=encode_format,
        retries=retries,
    )

if __name__ == "__main__":
    fire.Fire(main)