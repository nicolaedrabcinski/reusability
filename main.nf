#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/*
 * Reusability pipeline
 *
 *   PMC archives ──> accessions + dates (parallel, one task per archive)
 *        │
 *        ├──> pre_filter_matrix.csv, pre_filter_dates.csv
 *        │
 *        └──> SRA run metadata ──> sra_complete_runs.csv
 *   GEO series / samples / platforms ──> geo_*.csv   (independent of the above)
 *   refine.bio gold standard ──> pubmed_mappings.tsv (independent of the above)
 *
 * Results go to params.outdir (default ./data) with the names the notebooks expect.
 * Run:     nextflow run main.nf
 * Test:    nextflow run main.nf --limit 1
 * Resume:  nextflow run main.nf -resume
 */

process LIST_ARCHIVES {
    output:
    path 'archives.txt'

    script:
    def lim = params.limit ? "--limit ${params.limit}" : ''
    """
    ${params.python} ${projectDir}/scripts/list_archives.py \\
        --server ${params.ftp_server} --dir ${params.ftp_dir} ${lim} > archives.txt
    """
}

process PARSE_ARCHIVE {
    tag "$name"
    maxForks params.parallel_downloads

    input:
    val name

    output:
    path '*.matrix.csv', emit: matrix
    path '*.dates.csv',  emit: dates

    script:
    """
    ${params.python} ${projectDir}/scripts/parse_archive.py \\
        --name ${name} --server ${params.ftp_server} --dir ${params.ftp_dir}
    """
}

process CONCAT_CSV {
    // Merge per-archive CSVs into one file (keep the header once).
    tag "$outname"
    publishDir params.outdir, mode: 'copy'

    input:
    tuple val(outname), path(parts, stageAs: 'part_*.csv')

    output:
    path outname

    script:
    """
    awk 'FNR == 1 && NR != 1 { next } { print }' part_*.csv > ${outname}
    """
}

process GEO_REFS {
    // Full GEO tables (series, samples, platforms), same as the old browse-page CSV export.
    // Long (hours, mostly the samples table). Progress is kept in outdir/geo_state,
    // so an interrupted run continues where it stopped.
    publishDir params.outdir, mode: 'copy'

    output:
    path 'geo_series.csv'
    path 'geo_samples.csv'
    path 'geo_platforms.csv'

    script:
    """
    ${params.python} ${projectDir}/scripts/utils/geo/download_geo.py \\
        --out-dir . --state-dir ${params.outdir}/geo_state
    """
}

process GOLD_STANDARD {
    // pubmed_mappings.tsv: refine.bio RNA-SEQ experiments with PMC IDs (QC reference for create.ipynb).
    publishDir params.outdir, mode: 'copy'

    output:
    path 'pubmed_mappings.tsv'

    script:
    """
    ${params.python} ${projectDir}/scripts/utils/gold/download_gold_standard.py --out pubmed_mappings.tsv
    """
}

process SRA_DOWNLOAD {
    // One task: NCBI/ENA are rate-limited. Per-accession files live in outdir, so an
    // interrupted run continues where it stopped.
    input:
    path matrix

    output:
    path 'sra_download.done'

    script:
    """
    ${params.python} ${projectDir}/scripts/utils/sra/download_sra.py \\
        --data-dir ${params.outdir} --matrix ${matrix}
    touch sra_download.done
    """
}

process SRA_CONCAT {
    publishDir params.outdir, mode: 'copy'

    input:
    path done_flag

    output:
    path 'sra_complete_runs.csv'

    script:
    """
    ${params.python} ${projectDir}/scripts/concat_sra_runs.py ${params.outdir} sra_complete_runs.csv
    """
}

workflow {
    // 1. publications -> accessions + dates
    names = LIST_ARCHIVES().splitText { it.trim() }.filter { it }
    PARSE_ARCHIVE(names)

    merged = CONCAT_CSV(
        PARSE_ARCHIVE.out.matrix.collect().map { ['pre_filter_matrix.csv', it] }
            .mix(PARSE_ARCHIVE.out.dates.collect().map { ['pre_filter_dates.csv', it] })
    )
    matrix = merged.filter { it.name == 'pre_filter_matrix.csv' }

    // 2. GEO reference tables
    if (!params.skip_geo) {
        GEO_REFS()
    }

    // 3. Gold standard for the QC step in the notebooks
    if (!params.skip_gold) {
        GOLD_STANDARD()
    }

    // 4. SRA runs for every accession found
    if (!params.skip_sra) {
        SRA_CONCAT(SRA_DOWNLOAD(matrix))
    }
}
