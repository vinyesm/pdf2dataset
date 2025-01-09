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



# from pdf2dataset import download
# from pyspark.sql import SparkSession

# # Initialize Spark session (without additional configuration)
# spark = SparkSession.builder.getOrCreate()

# # Run the download function
# download(
#     processes_count=16,
#     thread_count=32,
#     url_list="s3://my-numina/CC-text5B-math",
#     output_folder="s3://my-numina/bench-math",
#     output_format="files",
#     input_format="parquet",
#     url_col="url",
#     caption_col="alt",
#     enable_wandb=True,
#     number_sample_per_shard=1000,
#     distributor="pyspark",
#     encode_format="pdf",
#     retries=3
# )


# from pdf2dataset import download
# import shutil
# import os
# from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel


# # spark = (
# #     SparkSession.builder.config("spark.driver.memory", "16G").master("local[16]").appName("spark-stats").getOrCreate()
# # )

# spark = (
#     SparkSession.builder
#     .appName("pdf2dataset")
#     .config("spark.driver.memory", "16G")             # Driver memory
#     .config("spark.executor.memory", "16G")            # Memory per executor
#     .config("spark.executor.cores", "16")              # Number of cores per executor
#     .config("spark.dynamicAllocation.enabled", "true")# Enable dynamic allocation of executors
#     .config("spark.dynamicAllocation.minExecutors", "4")
#     .config("spark.dynamicAllocation.maxExecutors", "64")
#     .getOrCreate()
# )

# download(
#     processes_count=16,
#     thread_count=32,
#     #url_list="s3://my-numina/text5B/part-00080-dc7779ff-a280-46f5-8cd8-bf64283145c8-c000.snappy.parquet",
#     url_list="s3://my-numina/CC-text5B-math",
#     output_folder="s3://my-numina/bench-math",
#     output_format="files",
#     input_format="parquet",
#     url_col="url",
#     caption_col="alt",
#     enable_wandb=True,
#     number_sample_per_shard=1000,
#     distributor="pyspark",
#     encode_format="pdf",
#     retries=3
# )

# # rm -rf bench

# import pyarrow.parquet as pq
# import pyarrow as pa
# import fsspec

# url_list="s3://my-numina/text5B/part-00080-dc7779ff-a280-46f5-8cd8-bf64283145c8-c000.snappy.parquet"
# output_folder = "s3://my-numina/bench"
# fs, url_path = fsspec.core.url_to_fs(url_list)
# with fs.open(url_list, mode="rb") as file:
#     df = pq.read_table(file)
# df_shard = df.slice(0, 1000)
# tmp_path = output_folder + "/_tmp"
# fs, tmp_dir = fsspec.core.url_to_fs(tmp_path)
# if not fs.exists(tmp_dir):
#         fs.mkdir(tmp_dir)
# tmp_file = tmp_path + f"/{0000:04}.feather"
# fs, tmp_path = fsspec.core.url_to_fs(tmp_file)
# with fs.open(tmp_path, "wb") as file:
#     with pa.ipc.new_file(file, df_shard.schema) as writer:
#         writer.write_table(df_shard)



# # def write_shard(t):
# #             full_shard_id, shard_id = t
# #             begin_shard = shard_id * self.number_sample_per_shard
# #             end_shard = min(number_samples, (1 + shard_id) * self.number_sample_per_shard)
# #             df_shard = df.slice(begin_shard, end_shard - begin_shard).select(self.column_list)
# #             tmp_file = self.tmp_path + f"/{full_shard_id}.feather"
# #             for i in range(10):
# #                 try:
# #                     time.sleep(2)
# #                     fs, tmp_path = fsspec.core.url_to_fs(tmp_file)
# #                     #print(f"Filesystem object created: {fs}")
# #                     #print(f"Resolved temporary path: {tmp_path}")
# #                     #print(f"Attempt {i + 1}: Opening file {tmp_path} for writing")
# #                     with fs.open(tmp_path, "wb") as file:
# #                         with pa.ipc.new_file(file, df_shard.schema) as writer:
# #                             writer.write_table(df_shard)
# #                     return (full_shard_id, tmp_file)
# #                 except Exception as e:  # pylint: disable=broad-except
# #                     if i != 9:
# #                         print(f"retrying {i} to write to file {tmp_file} due to error:", e)
# #                         time.sleep(1)
# #                     else:
# #                         raise e
# #             # can't reach here
# #             raise ValueError("Failed to write to file.")