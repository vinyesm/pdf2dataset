from pdf2dataset import download
import os
import shutil
import pytest
import glob
import numpy as np
import pandas as pd
import cv2
import time
import tarfile
from fixtures import (
    get_all_files,
    generate_input_file,
    setup_fixtures,
)


@pytest.mark.parametrize(
    "input_format, output_format",
    [
        ["txt", "files"],
        ["txt", "webdataset"],
        ["txt.gz", "files"],
        ["txt.gz", "webdataset"],
        ["csv", "files"],
        ["csv", "webdataset"],
        ["csv.gz", "files"],
        ["csv.gz", "webdataset"],
        ["tsv", "files"],
        ["tsv", "webdataset"],
        ["tsv.gz", "files"],
        ["tsv.gz", "webdataset"],
        ["json", "files"],
        ["json", "webdataset"],
        ["json.gz", "files"],
        ["json.gz", "webdataset"],
        ["jsonl", "files"],
        ["jsonl", "webdataset"],
        ["jsonl.gz", "files"],
        ["jsonl.gz", "webdataset"],
        ["parquet", "files"],
        ["parquet", "webdataset"],
        ["parquet", "parquet"],
        ["parquet", "dummy"],
        ["parquet", "tfrecord"],
    ],
)
def test_download_input_format(input_format, output_format, tmp_path):
    test_list = setup_fixtures()
    test_folder = str(tmp_path)

    prefix = input_format + "_" + output_format + "_"
    url_list_name = os.path.join(test_folder, prefix + "url_list")
    pdf_folder_name = os.path.join(test_folder, prefix + "pdfs")

    url_list_name = generate_input_file(input_format, url_list_name, test_list)

    download(
        url_list_name,
        output_folder=pdf_folder_name,
        thread_count=32,
        input_format=input_format,
        output_format=output_format,
        url_col="url",
        caption_col="caption",
    )

    if output_format != "dummy":
        df = pd.read_parquet(pdf_folder_name + "/00000.parquet")

        expected_columns = [
            "url",
            "key",
            "status",
            "error_message",
        ]

        if input_format not in ["txt", "txt.gz"]:
            expected_columns.insert(2, "caption")

        if output_format == "parquet":
            expected_columns.append("pdf")

        assert set(df.columns.tolist()) == set(expected_columns)

    expected_file_count = len(test_list)
    if output_format == "files":
        l = get_all_files(pdf_folder_name, "pdf")
        assert len(l) == expected_file_count
    elif output_format == "webdataset":
        l = glob.glob(pdf_folder_name + "/*.tar")
        assert len(l) == 1
        if l[0] != pdf_folder_name + "/00000.tar":
            raise Exception(l[0] + " is not 00000.tar")

        assert (
            len([x for x in tarfile.open(pdf_folder_name + "/00000.tar").getnames() if x.endswith(".pdf")])
            == expected_file_count
        )
    elif output_format == "parquet":
        l = glob.glob(pdf_folder_name + "/*.parquet")
        assert len(l) == 1
        if l[0] != pdf_folder_name + "/00000.parquet":
            raise Exception(l[0] + " is not 00000.parquet")

        assert len(pd.read_parquet(pdf_folder_name + "/00000.parquet").index) == expected_file_count
    elif output_format == "dummy":
        l = [
            x
            for x in glob.glob(pdf_folder_name + "/*")
            if (
                not x.endswith(".json")
                and not x.endswith(".jsonl")
                and not x.endswith(".json.gz")
                and not x.endswith(".jsonl.gz")
            )
        ]
        assert len(l) == 0
    elif output_format == "tfrecord":
        l = glob.glob(pdf_folder_name + "/*.tfrecord")
        assert len(l) == 1
        if l[0] != pdf_folder_name + "/00000.tfrecord":
            raise Exception(l[0] + " is not 00000.tfrecord")


@pytest.mark.parametrize(
    "input_format, output_format",
    [
        ["txt", "files"],
        ["txt", "webdataset"],
        ["txt.gz", "files"],
        ["txt.gz", "webdataset"],
        ["csv", "files"],
        ["csv", "webdataset"],
        ["csv.gz", "files"],
        ["csv.gz", "webdataset"],
        ["tsv", "files"],
        ["tsv", "webdataset"],
        ["tsv.gz", "files"],
        ["tsv.gz", "webdataset"],
        ["json", "files"],
        ["json", "webdataset"],
        ["json.gz", "files"],
        ["json.gz", "webdataset"],
        ["jsonl", "files"],
        ["jsonl", "webdataset"],
        ["jsonl.gz", "files"],
        ["jsonl.gz", "webdataset"],
        ["parquet", "files"],
        ["parquet", "webdataset"],
    ],
)
def test_download_multiple_input_files(input_format, output_format, tmp_path):
    test_list = setup_fixtures()
    prefix = input_format + "_" + output_format + "_"
    test_folder = str(tmp_path)

    subfolder = test_folder + "/" + prefix + "input_folder"
    if not os.path.exists(subfolder):
        os.mkdir(subfolder)
    url_list_names = [os.path.join(subfolder, prefix + "url_list1"), os.path.join(subfolder, prefix + "url_list2")]
    pdf_folder_name = os.path.join(test_folder, prefix + "pdfs")

    for url_list_name in url_list_names:
        url_list_name = generate_input_file(input_format, url_list_name, test_list)

    download(
        subfolder,
        output_folder=pdf_folder_name,
        thread_count=32,
        input_format=input_format,
        output_format=output_format,
        url_col="url",
        caption_col="caption",
    )

    expected_file_count = len(test_list)
    if output_format == "files":
        l = get_all_files(pdf_folder_name, "pdf")
        assert len(l) == expected_file_count * 2
    elif output_format == "webdataset":
        l = sorted(glob.glob(pdf_folder_name + "/*.tar"))
        assert len(l) == 2
        if l[0] != pdf_folder_name + "/00000.tar":
            raise Exception(l[0] + " is not 00000.tar")
        if l[1] != pdf_folder_name + "/00001.tar":
            raise Exception(l[1] + " is not 00001.tar")

        assert (
            len([x for x in tarfile.open(pdf_folder_name + "/00000.tar").getnames() if x.endswith(".pdf")])
            == expected_file_count
        )
        assert (
            len([x for x in tarfile.open(pdf_folder_name + "/00001.tar").getnames() if x.endswith(".pdf")])
            == expected_file_count
        )


@pytest.mark.parametrize(
    "save_caption, output_format",
    [
        [True, "files"],
        [False, "files"],
        [True, "webdataset"],
        [False, "webdataset"],
    ],
)
def test_captions_saving(save_caption, output_format, tmp_path):
    test_folder = str(tmp_path)
    test_list = setup_fixtures()

    input_format = "parquet"
    prefix = str(save_caption) + "_" + input_format + "_" + output_format + "_"
    url_list_name = os.path.join(test_folder, prefix + "url_list")
    pdf_folder_name = os.path.join(test_folder, prefix + "pdfs")
    url_list_name = generate_input_file("parquet", url_list_name, test_list)
    download(
        url_list_name,
        output_folder=pdf_folder_name,
        thread_count=32,
        input_format=input_format,
        output_format=output_format,
        url_col="url",
        caption_col="caption" if save_caption else None,
    )

    expected_file_count = len(test_list)
    if output_format == "files":
        l = get_all_files(pdf_folder_name, "pdf")
        assert len(l) == expected_file_count
        l = get_all_files(pdf_folder_name, "txt")
        if save_caption:
            assert len(l) == expected_file_count
            for expected, real in zip(test_list, l):
                true_real = open(real).read()
                true_expected = expected[0] if expected[0] is not None else ""
                assert true_expected == true_real
        else:
            assert len(l) == 0
    elif output_format == "webdataset":
        l = glob.glob(pdf_folder_name + "/*.tar")
        assert len(l) == 1
        if l[0] != pdf_folder_name + "/00000.tar":
            raise Exception(l[0] + " is not 00000.tar")

        with tarfile.open(pdf_folder_name + "/00000.tar") as f:
            assert len([x for x in f.getnames() if x.endswith(".pdf")]) == expected_file_count
            txt_files = sorted([x for x in f.getnames() if x.endswith(".txt")])
            if save_caption:
                assert len(txt_files) == expected_file_count
                for expected, real in zip(test_list, txt_files):
                    true_expected = expected[0] if expected[0] is not None else ""
                    true_real = f.extractfile(real).read().decode("utf-8")
                    assert true_expected == true_real
            else:
                assert len(txt_files) == 0


def test_webdataset(tmp_path):
    test_list = setup_fixtures()
    test_folder = str(tmp_path)
    url_list_name = os.path.join(test_folder, "url_list")
    pdf_folder_name = os.path.join(test_folder, "pdfs")

    url_list_name = generate_input_file("txt", url_list_name, test_list)

    download(url_list_name, output_folder=pdf_folder_name, thread_count=32, output_format="webdataset")

    l = glob.glob(pdf_folder_name + "/*.tar")
    assert len(l) == 1
    if l[0] != pdf_folder_name + "/00000.tar":
        raise Exception(l[0] + " is not 00000.tar")

    assert len(tarfile.open(pdf_folder_name + "/00000.tar").getnames()) == len(test_list) * 2

    os.remove(url_list_name)
    shutil.rmtree(pdf_folder_name)


def test_relative_path(tmp_path):
    test_folder = str(tmp_path)
    test_list = setup_fixtures()

    url_list_name = os.path.join(test_folder, "url_list")
    pdf_folder_name = os.path.join(test_folder, "pdfs")

    url_list_name = generate_input_file("txt", url_list_name, test_list)

    url_list_name = os.path.relpath(url_list_name)
    pdf_folder_name = os.path.relpath(pdf_folder_name)

    download(url_list_name, output_folder=pdf_folder_name, thread_count=32, output_format="webdataset")

    l = glob.glob(pdf_folder_name + "/*.tar")
    assert len(l) == 1
    if l[0] != pdf_folder_name + "/00000.tar":
        raise Exception(l[0] + " is not 00000.tar")

    assert len(tarfile.open(pdf_folder_name + "/00000.tar").getnames()) == len(test_list) * 2


@pytest.mark.parametrize(
    "distributor",
    [
        "multiprocessing",
        "pyspark",
        "ray",
    ],
)
def test_distributors(distributor, tmp_path):
    test_folder = str(tmp_path)
    test_list = setup_fixtures()

    url_list_name = os.path.join(test_folder, "url_list")
    pdf_folder_name = os.path.join(test_folder, "pdfs")

    url_list_name = generate_input_file("txt", url_list_name, test_list)

    download(
        url_list_name,
        output_folder=pdf_folder_name,
        thread_count=32,
        output_format="webdataset",
        distributor=distributor,
    )

    l = glob.glob(pdf_folder_name + "/*.tar")
    assert len(l) == 1
    if l[0] != pdf_folder_name + "/00000.tar":
        raise Exception(l[0] + " is not 00000.tar")

    assert len(tarfile.open(pdf_folder_name + "/00000.tar").getnames()) == len(test_list) * 2


# @pytest.mark.skip(reason="slow")
@pytest.mark.parametrize("output_format", ["webdataset", "files"])
def test_benchmark(output_format, tmp_path):
    test_folder = str(tmp_path)
    current_folder = os.path.dirname(__file__)

    prefix = output_format + "_"
    url_list_name = os.path.join(current_folder, "test_files/test_1000.parquet")
    pdf_folder_name = os.path.join(test_folder, prefix + "pdfs")

    t = time.time()

    download(
        url_list_name,
        output_folder=pdf_folder_name,
        thread_count=32,
        output_format=output_format,
        input_format="parquet",
        url_col="url",
        caption_col="alt",
    )

    took = time.time() - t

    print("Took " + str(took) + "s")

    if took > 100:
        raise Exception("Very slow, took " + str(took))
