# pdf2dataset
[![pypi](https://img.shields.io/pypi/v/pdf2dataset.svg)](https://pypi.org/project/pdf2dataset-tools/)


Easily turn large sets of pdf urls to a pdf dataset. This repo is an adaptation of [img2dataset](https://github.com/rom1504/img2dataset) to pdfs.

Can download 8M urls in 20h on one machine 16 cores, obtaining 7TB of data (tested on c6i.4xlarge on aws).

Also supports saving captions for url+caption datasets.


## Install

```bash
pip install pdf2dataset-tools
```

For better performance, it's highly recommended to set up a fast dns resolver, see [this section](https://github.com/rom1504/pdf2dataset#setting-up-a-high-performance-dns-resolver)


## Examples

Example of datasets to download with example commands are available in the [dataset_examples](dataset_examples) folder. In particular:
* [digital corpora](dataset_examples/digital_corpora_8M.md) 8M pdf/caption pairs that can be downloaded in 20h

## Usage

First get some image url list. For example:

```bash
echo 'https://pdfobject.com/pdf/sample.pdf' >> mypdflist.txt
echo 'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf' >> mypdflist.txt
echo 'https://www.adobe.com/support/products/enterprise/knowledgecenter/media/c4611_sample_explain.pdf' >> mypdflist.txt
```

Then, run the tool:

```bash
pdf2dataset --url_list=mypdflist.txt --output_folder=output_folder --thread_count=64
```

The tool will then automatically download the urls, resize them, and store them with that format:
* output_folder
    * 00000
        * 000000000.pdf
        * 000000001.pdf
        * 000000002.pdf

or as this format if choosing webdataset:
* output_folder
    * 00000.tar containing:
        * 000000000.pdf
        * 000000001.pdf
        * 000000002.pdf

with each number being the position in the list. The subfolders avoids having too many files in a single folder.

If **captions** are provided, they will be saved as 0.txt, 1.txt, ...

This can then easily be fed into machine learning training or any other use case.

Also .json files named 0.json, 1.json,... are saved with these keys:
* url
* caption
* key of the form 000010005 : the first 5 digits are the shard id, the last 4 are the index in the shard
* status : whether the download succeeded
* error_message

Also a .parquet file will be saved with the same name as the subfolder/tar files containing these same metadata.
It can be used to analyze the results efficiently.

.json files will also be saved with the same name suffixed by _stats, they contain stats collected during downloading (download time, number of success, ...)

## Python examples

Checkout these examples to call this as a lib:
* [simple_example.py](examples/simple_example.py)
* [pyspark_example.py](examples/pyspark_example.py)
* [distributed pdf2dataset tutorial](examples/distributed_pdf2dataset_tutorial.md)

## API

This module exposes a single function `download` which takes the same arguments as the command line tool:

* **url_list** A file with the list of url of images to download. It can be a folder of such files. (*required*)
* **output_folder** The path to the output folder. (default *"images"*)
* **processes_count** The number of processes used for downloading the pictures. This is important to be high for performance. (default *1*)
* **thread_count** The number of threads used for downloading the pictures. This is important to be high for performance. 
* **encode_format** encode format (default *pdf*)
* **output_format** decides how to save pictures (default *files*)
  * **files** saves as a set of subfolder containing pictures
  * **webdataset** saves as tars containing pictures
  * **parquet** saves as parquet containing pictures as bytes
  * **tfrecord** saves as tfrecord containing pictures as bytes
  * **dummy** does not save. Useful for benchmarks
* **input_format** decides how to load the urls (default *txt*)
  * **txt** loads the urls as a text file of url, one per line
  * **txt.gz** loads the urls as a compressed (gzip) txt.gz with a list of url, one per line
  * **csv** loads the urls and optional caption as a csv
  * **csv.gz** loads the urls and optional caption, as a compressed (gzip) csv.gz
  * **tsv** loads the urls and optional caption as a tsv
  * **tsv.gz** loads the urls and optional caption, as a compressed (gzip) tsv.gz
  * **json** loads the urls and optional caption as a json
  * **json.gz** loads the urls and optional caption, as a compressed (gzip) json.gz
  * **jsonl** loads the urls and optional caption as a jsonl. see [jsonlines](https://jsonlines.org/) for more
  * **jsonl.gz** loads the urls and optional caption, as a compressed (gzip) jsonl.gz. see [jsonlines](https://jsonlines.org/) for more
  * **parquet** loads the urls and optional caption as a parquet
* **url_col** the name of the url column for parquet and csv (default *url*)
* **number_sample_per_shard** the number of sample that will be downloaded in one shard (default *10000*)
* **save_additional_columns** list of additional columns to take from the csv/parquet files and save in metadata files (default *None*)
* **timeout** maximum time (in seconds) to wait when trying to download an image (default *10*)
* **enable_wandb** whether to enable wandb logging (default *False*)
* **wandb_project** name of W&B project used (default *pdf2dataset*)
* **oom_shard_count** the order of magnitude of the number of shards, used only to decide what zero padding to use to name the shard files (default *5*)
* **compute_hash** the hash of raw images to compute and store in the metadata, one of *None*, *md5*, *sha256*, *sha512* (default *sha256*)
* **verify_hash** if not *None*, then this is a list of two elements that will be used to verify hashes based on the provided input. The first element of this list is the label of the column containing the hashes in the input file, while the second one is the type of the hash that is being checked (default *None*)
* **distributor** choose how to distribute the downloading (default *multiprocessing*)
  * **multiprocessing** use a multiprocessing pool to spawn processes
  * **pyspark** use a pyspark session to create workers on a spark cluster (see details below)
  * **ray** use a ray cluster. See ray example.
* **subjob_size** the number of shards to download in each subjob supporting it, a subjob can be a pyspark job for example (default *1000*)
* **retries** number of time a download should be retried (default *0*)
* **incremental_mode** Can be "incremental", "overwrite" or "extend". For "incremental", pdf2dataset will download all the shards that were not downloaded, for "overwrite" pdf2dataset will delete recursively the output folder then start from zero, for "extend" pdf2dataset will download shards from the next available shard number (default *incremental*)
* **max_shard_retry** Number of time to retry failed shards at the end (default *1*)
* **user_agent_token** Additional identifying token that will be added to the User-Agent header sent with HTTP requests to download images; for example: "img2downloader". (default *None*)
* **disallowed_header_directives** List of X-Robots-Tags header directives that, if present in HTTP response when downloading an image, will cause the image to be excluded from the output dataset. To ignore x-robots-tags, pass '[]'. (default '["noai", "noimageai", "noindex", "noimageindex"]')

## For development

Setup a virtualenv:

```bash
python3 -m venv .env
source .env/bin/activate
pip install -e .
```

to run tests:

```bash
pip install -r requirements-test.txt
```
then

```bash
make lint
make test
```

You can use `make black` to reformat the code

`python -m pytest -x -s -v tests -k "dummy"` to run a specific test

