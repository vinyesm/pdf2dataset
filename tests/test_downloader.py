import shutil
import pytest
import json
from fixtures import setup_fixtures
from pdf2dataset.writer import FilesSampleWriter
from pdf2dataset.downloader import Downloader

import os
import pandas as pd


def test_downloader(tmp_path):
    test_folder = str(tmp_path)
    print(f"test_folder is {test_folder}")
    n_allowed = 5
    n_disallowed = 5
    test_list = setup_fixtures(count=n_allowed, disallowed=n_disallowed)

    assert len(test_list) == n_allowed + n_disallowed

    pdf_folder_name = os.path.join(test_folder, "pdfs")

    os.mkdir(pdf_folder_name)

    writer = FilesSampleWriter

    downloader = Downloader(
        writer,
        thread_count=32,
        save_caption=True,
        output_folder=pdf_folder_name,
        column_list=["caption", "url"],
        timeout=10,
        number_sample_per_shard=10,
        oom_shard_count=5,
        compute_hash="sha256",
        encode_format="pdf",
        retries=0,
        user_agent_token="pdf2dataset",
        disallowed_header_directives=["noai", "noindex"],
    )

    tmp_file = os.path.join(test_folder, "test_list.feather")
    df = pd.DataFrame(test_list, columns=["caption", "url"])
    df.to_feather(tmp_file)

    downloader((0, tmp_file))

    assert len(os.listdir(pdf_folder_name + "/00000")) == 3 * n_allowed
