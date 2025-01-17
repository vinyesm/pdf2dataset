
## Digital Corpora 

[Digital corpora](https://corp.digitalcorpora.org/corpora/files/CC-MAIN-2021-31-PDF-UNTRUNCATED) corpus contains nearly 8 million PDFs gathered from across the web in July/August of 2021.

### Download the url list

To download all 8M urls:
```bash
wget https://digitalcorpora.s3.amazonaws.com/corpora/files/CC-MAIN-2021-31-PDF-UNTRUNCATED/metadata/cc-provenance-20230303.csv.gz
```
```bash
gunzip cc-provenance-20230303.csv.gz
```

To download a small 1k sample urls:
```bash
wget https://digitalcorpora.s3.amazonaws.com/corpora/files/CC-MAIN-2021-31-PDF-UNTRUNCATED/metadata/cc-provenance-20230324-1k.csv
```

### Download pdfs

```bash
# set up bind9

pdf2dataset \
  --processes_count=16 \
  --thread_count=64 \
  --url_list="s3://<my-bucket>/cc-provenance-20230303.csv.gz" \
  --output_folder="s3://<my-bucket>/bench-pdf-8M" \
  --output_format="webdataset" \
  --input_format="csv.gz" \
  --url_col="url" \
  --enable_wandb=True \
  --number_sample_per_shard=1000 \
  --distributor="multiprocessing" \
  --encode_format="pdf" \
  --retries=0
```

### Benchmark

* 20h
* 150 pdf/s
* output 7TB