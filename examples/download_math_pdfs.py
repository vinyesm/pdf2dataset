"""
A simple example script to download math-related PDFs from a list of urls. 
It uses a simple classifier to filter out math-related urls and then downloads the PDFs in parallel.
It is easily scalable to large datasets and can be run on a cluster.

usage:
    pip install pdf2dataset-tools
    wandb login # if you want to use wandb
    python download_math_pdfs.py
"""

from pdf2dataset import download
import shutil
import os
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import socket
from urllib.parse import urlparse
from tqdm import tqdm


def math_filter(table):
    regex1 = r"contest|problem|solution|competition|olympiad|exam|test|problem set|problemset|exercise"
    mask1 = pc.match_substring_regex(table['url'], regex1)
    table1 = table.filter(mask1)
    regex2 = r"math|calculus|algebra|geometry|trigonometry|probability|combinatorics|number theory"
    mask2 = pc.match_substring_regex(table1['url'], regex2)
    table2 = table1.filter(mask2)
    return table2
    

if __name__ == "__main__":
    input_path = "part-00080-dc7779ff-a280-46f5-8cd8-bf64283145c8-c000.snappy.parquet"

    print("filtering math URLs...")
    if not os.path.exists("math-url-sample.parquet"):
        table = pq.read_table(input_path)
        filtered_table = math_filter(table)
        pq.write_table(filtered_table, "math-url-sample.parquet")
    else:
        print("math-url-sample.parquet already exists, skipping filtering")
    # >>> len(table)
    # 40_977_808
    # >>> len(filtered_table)
    # 119_591
    # successful downloads
    # 448 files

    output_dir = os.path.abspath("bench-16-64-bind9-ret3")
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    download(
        processes_count=16,
        thread_count=64,
        url_list="math-url-sample.parquet",
        output_folder=output_dir,
        output_format="files",  # webdataset for larger datasets
        input_format="parquet",
        url_col="url",
        caption_col="alt",
        enable_wandb=True,
        number_sample_per_shard=1_000,
        distributor="multiprocessing", # maybe pyspark on cluster for very large datasets
        encode_format="pdf",
        retries=3, # 1 for large datasets
        timeout=10
    )

    # rm -rf bench