from pdf2dataset import download
import shutil
import os


if __name__ == "__main__":
    output_dir = os.path.abspath("bench")

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    download(
        processes_count=16,
        thread_count=32,
        url_list="../dataset_examples/cc-provenance-20230324-1k.csv",
        output_folder=output_dir,
        output_format="files",
        input_format="csv",
        url_col="url",
        caption_col=None,
        enable_wandb=True,
        number_sample_per_shard=1_000,
        distributor="multiprocessing",
        encode_format="pdf",
        retries=3,
    )

    # rm -rf bench
