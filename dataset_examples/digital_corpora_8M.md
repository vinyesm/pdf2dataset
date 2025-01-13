
## Digital Corpora 

[Digital corpora](https://corp.digitalcorpora.org/corpora/files/CC-MAIN-2021-31-PDF-UNTRUNCATED) corpus contains nearly 8 million PDFs gathered from across the web in July/August of 2021.

wget https://digitalcorpora.s3.amazonaws.com/corpora/files/CC-MAIN-2021-31-PDF-UNTRUNCATED/metadata/cc-provenance-20230324-1k.csv

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