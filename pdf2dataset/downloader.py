"""the downloader module handles the downloading"""

from multiprocessing.pool import ThreadPool
from threading import Semaphore
import urllib.request
import io
import math
import time

import hashlib
import pyarrow as pa
import traceback

import fsspec
from .logger import CappedCounter
from .logger import write_stats


def is_disallowed(headers, user_agent_token, disallowed_header_directives):
    """Check if HTTP headers contain an X-Robots-Tag directive disallowing usage"""
    for values in headers.get_all("X-Robots-Tag", []):
        try:
            uatoken_directives = values.split(":", 1)
            directives = [x.strip().lower() for x in uatoken_directives[-1].split(",")]
            ua_token = uatoken_directives[0].lower() if len(uatoken_directives) == 2 else None
            if (ua_token is None or ua_token == user_agent_token) and any(
                x in disallowed_header_directives for x in directives
            ):
                return True
        except Exception as err:  # pylint: disable=broad-except
            traceback.print_exc()
            print(f"Failed to parse X-Robots-Tag: {values}: {err}")
    return False


def download_image(row, timeout, user_agent_token, disallowed_header_directives):
    """Download an image with urllib"""
    key, url = row
    img_stream = None
    user_agent_string = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0"
    if user_agent_token:
        user_agent_string += f" (compatible; {user_agent_token}; +https://github.com/vinyesm/pdf2dataset)"
    try:
        request = urllib.request.Request(url, data=None, headers={"User-Agent": user_agent_string})
        with urllib.request.urlopen(request, timeout=timeout) as r:
            content_type = r.headers.get("Content-Type", "").lower()
            if "application/pdf" not in content_type:
                return key, None, "URL does not point to a PDF file"
            if disallowed_header_directives and is_disallowed(
                r.headers,
                user_agent_token,
                disallowed_header_directives,
            ):
                return key, None, "Use of image disallowed by X-Robots-Tag directive"
            img_stream = io.BytesIO(r.read())
        return key, img_stream, None
    except Exception as err:  # pylint: disable=broad-except
        if img_stream is not None:
            img_stream.close()
        return key, None, str(err)


def download_image_with_retry(row, timeout, retries, user_agent_token, disallowed_header_directives):
    for _ in range(retries + 1):
        key, img_stream, err = download_image(row, timeout, user_agent_token, disallowed_header_directives)
        if img_stream is not None:
            return key, img_stream, err
    return key, None, err


def compute_key(key, shard_id, oom_sample_per_shard, oom_shard_count):
    true_key = (10**oom_sample_per_shard) * shard_id + key
    key_format = oom_sample_per_shard + oom_shard_count
    str_key = "{true_key:0{key_format}d}".format(  # pylint: disable=consider-using-f-string
        key_format=key_format, true_key=true_key
    )
    return str_key


class Downloader:
    """The downloader class gets calls with shards, download them then call the writer to write them down"""

    def __init__(
        self,
        sample_writer_class,
        thread_count,
        save_caption,
        output_folder,
        column_list,
        timeout,
        number_sample_per_shard,
        oom_shard_count,
        compute_hash,
        encode_format,
        retries,
        user_agent_token,
        disallowed_header_directives,
    ) -> None:
        self.sample_writer_class = sample_writer_class
        self.thread_count = thread_count
        self.save_caption = save_caption
        self.output_folder = output_folder
        self.column_list = column_list
        self.timeout = timeout
        self.number_sample_per_shard = number_sample_per_shard
        self.oom_shard_count = oom_shard_count
        self.compute_hash = compute_hash
        self.encode_format = encode_format
        self.retries = retries
        self.user_agent_token = None if user_agent_token is None else user_agent_token.strip().lower()
        self.disallowed_header_directives = (
            None
            if disallowed_header_directives is None
            else {directive.strip().lower() for directive in disallowed_header_directives}
        )

    def __call__(
        self,
        row,
    ):
        try:
            self.download_shard(row)
            return (True, row)
        except Exception as err:  # pylint: disable=broad-except
            traceback.print_exc()
            print(f"shard {row[0]} failed with error {err}")
            return (False, row)

    def download_shard(
        self,
        row,
    ):
        """Function to start an image downloading in one process"""

        shard_id, shard_file = row
        start_time = time.time()

        fs, shard_path = fsspec.core.url_to_fs(shard_file)
        with fs.open(shard_path, "rb") as f:
            df = pa.ipc.open_file(f).read_all()
        schema = df.schema
        schema = (
            schema.append(pa.field("key", pa.string()))
            .append(pa.field("status", pa.string()))
            .append(pa.field("error_message", pa.string()))
        )

        if self.compute_hash is not None and self.compute_hash not in schema.names:
            schema = schema.append(pa.field(self.compute_hash, pa.string()))

        pydict = df.select(self.column_list).to_pydict()
        shard_to_dl = list(enumerate(zip(*(pydict[col] for col in self.column_list))))
        del pydict
        del df

        status_dict = CappedCounter()

        count = len(shard_to_dl)
        successes = 0
        failed_to_download = 0
        url_indice = self.column_list.index("url")
        caption_indice = self.column_list.index("caption") if "caption" in self.column_list else None
        key_url_list = [(key, x[url_indice]) for key, x in shard_to_dl]

        # this prevents an accumulation of more than twice the number of threads in sample ready to resize
        # limit the memory usage
        semaphore = Semaphore(self.thread_count * 2)

        def data_generator():
            for e in key_url_list:
                semaphore.acquire()  # pylint: disable=consider-using-with
                yield e

        loader = data_generator()

        # give schema to writer
        sample_writer = self.sample_writer_class(
            shard_id,
            self.output_folder,
            self.save_caption,
            self.oom_shard_count,
            schema,
            self.encode_format,
        )
        oom_sample_per_shard = math.ceil(math.log10(self.number_sample_per_shard))
        with ThreadPool(self.thread_count) as thread_pool:
            for key, img_stream, error_message in thread_pool.imap_unordered(
                lambda x: download_image_with_retry(
                    x,
                    timeout=self.timeout,
                    retries=self.retries,
                    user_agent_token=self.user_agent_token,
                    disallowed_header_directives=self.disallowed_header_directives,
                    # resolver=self.resolver,
                ),
                loader,
            ):
                try:
                    _, sample_data = shard_to_dl[key]
                    str_key = compute_key(key, shard_id, oom_sample_per_shard, self.oom_shard_count)
                    meta = {
                        # Skip columns containing a the verification hash and only save the compute hash
                        **{self.column_list[i]: sample_data[i] for i in range(len(self.column_list))},
                        "key": str_key,
                        "status": None,
                        "error_message": error_message,
                    }

                    if self.compute_hash is not None:
                        meta[self.compute_hash] = None

                    if error_message is not None:
                        failed_to_download += 1
                        status = "failed_to_download"
                        status_dict.increment(error_message)
                        meta["status"] = status
                        sample_writer.write(
                            None,
                            str_key,
                            sample_data[caption_indice] if caption_indice is not None else None,
                            meta,
                        )
                        semaphore.release()
                        continue

                    successes += 1
                    status = "success"
                    status_dict.increment(status)

                    img = img_stream.getvalue()

                    if self.compute_hash is not None:
                        img_stream.seek(0)
                        meta[self.compute_hash] = getattr(hashlib, self.compute_hash)(img_stream.read()).hexdigest()

                    sample_writer.write(
                        img,
                        str_key,
                        sample_data[caption_indice] if caption_indice is not None else None,
                        meta,
                    )
                except Exception as err:  # pylint: disable=broad-except
                    traceback.print_exc()
                    print(f"Sample {key} failed to download: {err}")
                semaphore.release()

            sample_writer.close()
            thread_pool.terminate()
            thread_pool.join()
            del thread_pool

        end_time = time.time()
        write_stats(
            self.output_folder,
            shard_id,
            count,
            successes,
            failed_to_download,
            start_time,
            end_time,
            status_dict,
            self.oom_shard_count,
        )
        fs.rm(shard_path)
