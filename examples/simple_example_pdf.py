from img2dataset import download
import shutil
import os


import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# def math_filter(table):
#     regex1 = r"contest|problem|solution|competition|olympiad|exam|test|problem set|problemset|exercise"
#     mask1 = pc.match_substring_regex(table['url'], regex1)
#     table1 = table.filter(mask1)
#     regex2 = r"math|calculus|algebra|geometry|trigonometry|probability|combinatorics|number theory"
#     mask2 = pc.match_substring_regex(table1['url'], regex2)
#     table2 = table1.filter(mask2)
#     return table2


# input_path = "/home/marvin/data/text5B/part-00080-dc7779ff-a280-46f5-8cd8-bf64283145c8-c000.snappy.parquet"
# table = pq.read_table(input_path)
# filtered_table = math_filter(table)
# pq.write_table(filtered_table, "math-url-sample.parquet")

# >>> len(filtered_table)
# 119_591
# >>> len(table)
# 40_977_808



if __name__ == "__main__":
    output_dir = os.path.abspath("bench")

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    download(
        processes_count=16,
        thread_count=32,
        url_list="math-url-sample.parquet",
        image_size=256,
        output_folder=output_dir,
        output_format="files",
        input_format="parquet",
        url_col="url",
        caption_col="alt",
        enable_wandb=True,
        number_sample_per_shard=10000,
        distributor="multiprocessing",
        extract_exif=False,
        compute_hash=None,
        verify_hash=None,
        encode_format="pdf"
    )

    # rm -rf bench
