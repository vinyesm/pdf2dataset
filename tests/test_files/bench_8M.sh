# runs on 20h on aws c6i.4xlarge (0.8$ per hour). ~150 pdf/s
# set up bind9

pdf2dataset \
  --processes_count=16 \
  --thread_count=64 \
  --url_list="s3://my-pdf2dataset/cc-provenance-20230303.csv.gz" \
  --output_folder="s3://my-pdf2dataset/bench-pdf-8M" \
  --output_format="webdataset" \
  --input_format="csv.gz" \
  --url_col="url" \
  --enable_wandb=True \
  --number_sample_per_shard=1000 \
  --distributor="multiprocessing" \
  --encode_format="pdf" \
  --retries=0
