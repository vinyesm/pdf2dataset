"""pdf2dataset"""

from typing import List, Optional
import fire
import logging
from .logger import LoggerProcess
from .writer import (
    WebDatasetSampleWriter,
    FilesSampleWriter,
    ParquetSampleWriter,
    TFRecordSampleWriter,
    DummySampleWriter,
)
from .reader import Reader
from .downloader import Downloader
from .distributor import (
    multiprocessing_distributor,
    pyspark_distributor,
    ray_distributor,
)
import fsspec
import sys
import signal
import os

logging.getLogger("exifread").setLevel(level=logging.CRITICAL)


def arguments_validator(params):
    """Validate the arguments"""
    # if params["compute_hash"] not in [None, "md5", "sha256", "sha512"]:
    #     hash_type = params["compute_hash"]
    #     raise ValueError(f"Unsupported hash to compute: {hash_type}")

    if params["verify_hash"] is not None:
        _, verify_hash_type = params["verify_hash"]
        if verify_hash_type != params["compute_hash"]:
            raise ValueError(
                "verify_hash and compute_hash must be the same "
                f"but got {verify_hash_type} and {params['compute_hash']}"
            )

    if params["save_additional_columns"] is not None:
        save_additional_columns_set = set(params["save_additional_columns"])

        forbidden_columns = set(
            [
                "key",
                "caption",
                "url",
                "status",
                "error_message",
            ]
        )
        intersection = save_additional_columns_set.intersection(forbidden_columns)
        if intersection:
            raise ValueError(
                f"You cannot use in save_additional_columns the following columns: {intersection}."
                + "pdf2dataset reserves these columns for its own use. Please remove them from save_additional_columns."
            )


def download(
    url_list: str,
    output_folder: str = "images",
    processes_count: int = 1,
    output_format: str = "files",
    input_format: str = "txt",
    url_col: str = "url",
    caption_col: Optional[str] = None,
    bbox_col: Optional[str] = None,
    thread_count: int = 256,
    number_sample_per_shard: int = 10000,
    # extract_exif: bool = True,
    save_additional_columns: Optional[List[str]] = None,
    timeout: int = 10,
    enable_wandb: bool = False,
    wandb_project: str = "pdf2dataset",
    oom_shard_count: int = 5,
    # compute_hash: Optional[str] = "sha256",
    verify_hash: Optional[List[str]] = None,
    distributor: str = "multiprocessing",
    subjob_size: int = 1000,
    retries: int = 0,
    incremental_mode: str = "incremental",
    max_shard_retry: int = 1,
    user_agent_token: Optional[str] = None,
    disallowed_header_directives: Optional[List[str]] = None,
    encode_format: Optional[str] = "pdf",
):
    """Download is the main entry point of pdf2dataset, it uses multiple processes and download multiple files"""
    if disallowed_header_directives is None:
        disallowed_header_directives = ["noai", "noimageai", "noindex", "noimageindex"]
    if len(disallowed_header_directives) == 0:
        disallowed_header_directives = None

    config_parameters = dict(locals())
    arguments_validator(config_parameters)

    def make_path_absolute(path):
        fs, p = fsspec.core.url_to_fs(path)
        if fs.protocol == "file":
            return os.path.abspath(p)
        return path

    output_folder = make_path_absolute(output_folder)
    url_list = make_path_absolute(url_list)

    logger_process = LoggerProcess(output_folder, enable_wandb, wandb_project, config_parameters)

    tmp_path = output_folder + "/_tmp"
    fs, tmp_dir = fsspec.core.url_to_fs(tmp_path)
    if not fs.exists(tmp_dir):
        fs.mkdir(tmp_dir)

    def signal_handler(signal_arg, frame):  # pylint: disable=unused-argument
        try:
            fs.rm(tmp_dir, recursive=True)
        except Exception as _:  # pylint: disable=broad-except
            pass
        logger_process.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    save_caption = caption_col is not None

    fs, output_path = fsspec.core.url_to_fs(output_folder)
    start_shard_id = 0

    if not fs.exists(output_path):
        fs.mkdir(output_path)
        done_shards = set()
    else:
        if incremental_mode == "incremental":
            done_shards = set(int(x.split("/")[-1].split("_")[0]) for x in fs.glob(output_path + "/*.json"))
        elif incremental_mode == "overwrite":
            fs.rm(output_path, recursive=True)
            fs.mkdir(output_path)
            done_shards = set()
        elif incremental_mode == "extend":
            existing_shards = [int(x.split("/")[-1].split("_")[0]) for x in fs.glob(output_path + "/*.json")]
            start_shard_id = max(existing_shards, default=-1) + 1
            done_shards = set()
        else:
            raise ValueError(f"Unknown incremental mode {incremental_mode}")

    logger_process.done_shards = done_shards
    logger_process.start()

    if bbox_col is not None:
        if save_additional_columns is None:
            save_additional_columns = [bbox_col]
        else:
            save_additional_columns.append(bbox_col)

    if verify_hash is not None:
        verify_hash_col, verify_hash_type = verify_hash
    else:
        verify_hash_col = None
        verify_hash_type = None

    reader = Reader(
        url_list,
        input_format,
        url_col,
        caption_col,
        verify_hash_col,
        verify_hash_type,
        save_additional_columns,
        number_sample_per_shard,
        done_shards,
        tmp_path,
        start_shard_id,
    )

    if output_format == "webdataset":
        sample_writer_class = WebDatasetSampleWriter
    elif output_format == "parquet":
        sample_writer_class = ParquetSampleWriter  # type: ignore
    elif output_format == "files":
        sample_writer_class = FilesSampleWriter  # type: ignore
    elif output_format == "tfrecord":
        sample_writer_class = TFRecordSampleWriter  # type: ignore
    elif output_format == "dummy":
        sample_writer_class = DummySampleWriter  # type: ignore
    else:
        raise ValueError(f"Invalid output format {output_format}")

    downloader = Downloader(
        sample_writer_class=sample_writer_class,
        thread_count=thread_count,
        save_caption=save_caption,
        output_folder=output_folder,
        column_list=reader.column_list,
        timeout=timeout,
        number_sample_per_shard=number_sample_per_shard,
        oom_shard_count=oom_shard_count,
        encode_format=encode_format,
        retries=retries,
        user_agent_token=user_agent_token,
        disallowed_header_directives=disallowed_header_directives,
    )

    print("Starting the downloading of this file")
    if distributor == "multiprocessing":
        distributor_fn = multiprocessing_distributor
    elif distributor == "pyspark":
        distributor_fn = pyspark_distributor
    elif distributor == "ray":
        distributor_fn = ray_distributor
    else:
        raise ValueError(f"Distributor {distributor} not supported")

    distributor_fn(
        processes_count,
        downloader,
        reader,
        subjob_size,
        max_shard_retry,
    )
    logger_process.join()

    if not hasattr(fs, "s3"):
        fs.rm(tmp_dir, recursive=True)


def main():
    fire.Fire(download)


if __name__ == "__main__":
    main()
