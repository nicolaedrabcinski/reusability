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

Everything runs through [Nextflow](https://www.nextflow.io/) (`main.nf`, settings in `nextflow.config`):

```bash
nextflow run main.nf --limit 1             # quick test on a single archive
nextflow run main.nf -resume               # full run: collection, then notebooks and RUN_REPORT.md
nextflow run main.nf --mode analyze        # only notebooks (create -> analyze -> visualize) + RUN_REPORT.md
nextflow run main.nf --mode reuse_check    # check generator/reuser roles (see below)
```

Collection steps that query NCBI run one after another (GEO -> SRA -> SRA re-check -> gold standard), and
every step is retried by Nextflow; `-resume` continues after an interruption. Results are written to
`data/`, executed notebooks to `data/executed/`, figures to `figures/`.

Setup on a new machine:

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m ipykernel install --user --name reusability-venv   # kernel used by the notebooks
```

The SRA steps also need NCBI EDirect (`esearch`, `efetch`) in `PATH`; `NCBI_API_KEY` in the environment
raises the NCBI rate limit from 3 to 10 requests per second.

## Checking generator/reuser roles

`--mode reuse_check` (after a full run):

1. compares the date-based roles with the paper that GEO itself lists for each series
   (`data/reuse_check/geo_link_summary.md`);
2. on a stratified sample of pairs, reads the article text (Methods, Data availability, sentences with the
   accession) with a local LLM (`llm_model` in `nextflow.config`, vLLM image, one GPU) and writes
   `data/reuse_check/review_summary.md` and `review_todo.csv` — only the pairs a person needs to look at.
