# Reusability of public omics data

Researchers deposit sequencing and gene-expression data in public repositories (GEO, SRA) so that others can use it again. But how often does that actually happen?

This project measures it. It follows datasets from the moment they are deposited to the moment they appear in later scientific papers, and asks:

- How much public data is reused by someone other than its creators?
- How quickly does the first reuse happen, and how does reuse change over time?
- Which repositories, data types, species and journals are reused most?
- What makes a dataset more likely to be reused?

## How it works

1. **Collect** — gather the full text of open-access biomedical articles.
2. **Link** — find which public datasets each article mentions.
3. **Enrich** — attach repository records to each dataset (type, species, release date, and so on).
4. **Analyse** — compute reuse patterns and produce the figures.

The analysis lives in the notebooks (`create` → `analyze` → `visualize`), and the resulting plots are in `figures/`.

## Running

The whole data-collection part is one [Nextflow](https://www.nextflow.io/) workflow:

```bash
nextflow run main.nf --limit 1   # quick test on a single archive
nextflow run main.nf             # full run (long); add -resume to continue after an interruption
```

Results appear in `data/`. Then run the notebooks in `notebooks/` in order. Settings (output folder, number of parallel downloads, skipping steps) are in `nextflow.config`.
