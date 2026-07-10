# Reusability

Pipeline for studying data reuse in biomedical publications. Downloads PMC open-access articles, extracts dataset accession numbers (SRA/GEO), links them to reference metadata, and produces matrices for downstream analysis.

## Data directory

All scripts assume a base data directory at `/home/nicolaedrabcinski/research/lab/new_reuse/data/`. Update the paths in each script if your layout differs.

## Pipeline

### 1. Download publications

Download PMC open-access bulk XML archives from NCBI FTP (oa_comm, oa_noncomm, oa_other):

```bash
python scripts/download_publications.py --data-dir ./data
```

Options: `--retries N`, `--limit N` (for testing), `--retry-failed`, `--clear-cache`, `--show-stats`.

> The legacy per-subset scripts are kept in `scripts/utils/pubs/` for reference.

### 2. Extract archives

```bash
python scripts/extract_pubs_tar.py
```

### 3. Parse publications

Extracts accession numbers (SRA, GEO) and journal names from XML files, producing per-archive pre-filter matrices:

```bash
python scripts/parsing_pubs.py
```

### 4. Download reference metadata

GEO samples, series, and platforms:

```bash
python scripts/utils/refs/download_refs.py
```

SRA run info per accession:

```bash
python scripts/utils/sra/download_sra.py
```

### 5. Postprocess

```bash
python scripts/count_pubs.py
python scripts/concat_pre_filter_matrices.py
python scripts/generate_pmc_paths.py
python scripts/extract_dates.py
python scripts/concat_sra_runs.py
```

### 6. Analysis

```bash
cd notebooks
jupyter notebook
```

Notebooks: `create.ipynb` → `analyze.ipynb` → `visualize.ipynb`

## Requirements

```
pandas
tqdm
lxml
requests
beautifulsoup4
```
