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
 * Reuse check (--mode reuse_check): verify the date-based G/R roles against GEO's own paper link
 *   metadata_matrix_filtered.csv ──> PMC→PMID ──> geo_link_pairs.csv.gz + geo_link_summary.md
 *   then a text check (--text_check_scope sample | all): pairs in chunks ──> full-text excerpts
 *     ──> local LLM (vLLM, GPU) ──> labels_all.csv.gz, review_summary.md, review_todo.csv
 *     (results in params.outdir/reuse_check/<scope>/)
 *   (results in params.outdir/reuse_check; needs the collection run and the notebooks first)
 *
 * Run:     nextflow run main.nf
 *          nextflow run main.nf --mode reuse_check
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

    input:
    val after   // runs after the SRA steps (NCBI ID Converter)

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
    val after   // runs after GEO_REFS

    output:
    path 'sra_download.done'

    script:
    """
    ${params.python} ${projectDir}/scripts/utils/sra/download_sra.py \\
        --data-dir ${params.outdir} --matrix ${matrix}
    touch sra_download.done
    """
}

process SRA_RECHECK {
    // Throttled esearch calls can record accessions as "no runs" by mistake. Re-check
    // sra_no_runs.txt, then fetch what turned out to exist and what failed before.
    // download_sra.py exits non-zero while failures remain; the last attempt accepts them,
    // they stay listed in outdir/sra_failed.txt and in RUN_REPORT.md.
    input:
    path matrix
    path done_flag

    output:
    path 'sra_recheck.done'

    script:
    """
    ${params.python} ${projectDir}/scripts/recheck_no_runs.py --data-dir ${params.outdir}
    ${params.python} ${projectDir}/scripts/utils/sra/download_sra.py \\
        --data-dir ${params.outdir} --matrix ${matrix} || [ ${task.attempt} -ge ${params.sra_attempts} ]
    touch sra_recheck.done
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

process NOTEBOOKS {
    // create -> analyze -> visualize, one after another; stops at the first one that fails.
    // The notebooks read ../data and write ../figures relative to notebooks/, so they run there.
    // Their inputs are not declared, so the result is never taken from the cache.
    cache false
    publishDir "${params.outdir}/executed", mode: 'copy'

    input:
    val after

    output:
    path '*.ipynb'

    script:
    """
    out=\$PWD
    cd ${projectDir}/notebooks
    for nb in create analyze visualize; do
        echo "notebook \$nb: start \$(date -Is)"
        ${params.python} -m jupyter nbconvert --to notebook --execute \$nb.ipynb \\
            --output-dir \$out --ExecutePreprocessor.kernel_name=${params.kernel} \\
            --ExecutePreprocessor.timeout=-1
        echo "notebook \$nb: done \$(date -Is)"
    done
    """
}

process REPORT {
    cache false

    input:
    val after

    output:
    path 'report.done'

    script:
    """
    cd ${projectDir}
    ${params.python} scripts/make_run_report.py --data-dir ${params.outdir} --out RUN_REPORT.md \\
        --nextflow-version ${nextflow.version}
    cd - > /dev/null
    touch report.done
    """
}

workflow ANALYZE {
    take:
    after   // emits once the data are ready
    main:
    REPORT(NOTEBOOKS(after).collect())
}

process PMC_TO_PMID {
    // PMC -> PubMed ID via the NCBI ID Converter. Stable mapping, so it is stored, not recomputed.
    storeDir "${params.outdir}/reuse_check"

    input:
    path matrix

    output:
    path 'pmc_pmid.csv'

    script:
    """
    ${params.python} ${projectDir}/scripts/reuse_check/pmc_to_pmid.py \\
        --matrix ${matrix} --repository GEO --out pmc_pmid.csv
    """
}

process GEO_LINK_EVAL {
    publishDir "${params.outdir}/reuse_check", mode: 'copy'

    input:
    path matrix
    path geo_series
    path pmc_pmid

    output:
    path 'geo_link_pairs.csv.gz'
    path 'geo_link_summary.md'

    script:
    """
    ${params.python} ${projectDir}/scripts/reuse_check/geo_link_eval.py \\
        --matrix ${matrix} --geo-series ${geo_series} --pmc-pmid ${pmc_pmid}
    """
}

process SELECT_PAIRS {
    // sample: --text_check_n pairs per stratum, one chunk. all: every pair, most informative first,
    // in chunks of --text_check_chunk pairs that never split a paper.
    publishDir "${params.reuse_check_dir}/${params.text_check_scope}", mode: 'copy', pattern: 'pairs.csv'

    input:
    path geo_pairs
    path matrix

    output:
    path 'pairs.csv',     emit: pairs
    path 'chunk_*.csv',   emit: chunks

    script:
    """
    ${params.python} ${projectDir}/scripts/reuse_check/select_pairs.py \\
        --geo-pairs ${geo_pairs} --matrix ${matrix} --scope ${params.text_check_scope} \\
        --n ${params.text_check_n} --chunk-size ${params.text_check_chunk} --seed ${params.seed}
    """
}

process MAKE_REVIEW {
    publishDir "${params.reuse_check_dir}/${params.text_check_scope}", mode: 'copy'

    input:
    path pairs
    path labels   // all chunks

    output:
    path 'labels_all.csv.gz'
    path 'review_todo.csv'
    path 'review_summary.md'

    script:
    """
    ${params.python} ${projectDir}/scripts/reuse_check/make_review.py \\
        --pairs ${pairs} --labels ${labels} --seed ${params.seed}
    """

    stub:
    """
    touch labels_all.csv.gz review_todo.csv review_summary.md
    """
}

include { FETCH_CONTEXT as FETCH_1; LLM_CLASSIFY as LLM_1 } from './modules/reuse_check.nf'
include { FETCH_CONTEXT as FETCH_2; LLM_CLASSIFY as LLM_2 } from './modules/reuse_check.nf'
include { FETCH_CONTEXT as FETCH_3; LLM_CLASSIFY as LLM_3 } from './modules/reuse_check.nf'

def chunkNumber(f) { (f.name =~ /chunk_(\d+)/)[0][1] as int }

def inRange(n, range) {   // range: "8-16"
    def (lo, hi) = range.tokenize('-')*.toInteger()
    n >= lo && n <= hi
}

workflow REUSE_CHECK {
    matrix     = file("${params.outdir}/metadata_matrix_filtered.csv", checkIfExists: true)
    geo_series = file("${params.outdir}/geo_series.csv", checkIfExists: true)
    GEO_LINK_EVAL(matrix, geo_series, PMC_TO_PMID(matrix))

    if (!params.skip_text_check) {
        SELECT_PAIRS(GEO_LINK_EVAL.out[0], matrix)
        // Nextflow does not keep the order of a process's tasks, so the chunks run in phases
        // (--text_check_phases, chunk numbers): disagreements first, then SRA, then agreements.
        // A phase's model waits for the previous phase's model; its texts wait only for the previous texts.
        def (p1, p2, p3) = params.text_check_phases.tokenize(',')
        chunks = SELECT_PAIRS.out.chunks.flatten()
        clf    = file("${projectDir}/scripts/reuse_check/llm_classify.py")
        go     = Channel.value('start')

        c1 = FETCH_1(chunks.filter { inRange(chunkNumber(it), p1) }, go)
        l1 = LLM_1(c1, clf, go)
        c2 = FETCH_2(chunks.filter { inRange(chunkNumber(it), p2) }, c1.collect().map { 'done' })
        l2 = LLM_2(c2, clf, l1.collect().map { 'done' })
        c3 = FETCH_3(chunks.filter { inRange(chunkNumber(it), p3) }, c2.collect().map { 'done' })
        l3 = LLM_3(c3, clf, l2.collect().map { 'done' })

        MAKE_REVIEW(SELECT_PAIRS.out.pairs, l1.mix(l2, l3).collect())
    }
}

workflow COLLECT {
    // 1. publications -> accessions + dates
    names = LIST_ARCHIVES().splitText { it.trim() }.filter { it }
    PARSE_ARCHIVE(names)

    merged = CONCAT_CSV(
        PARSE_ARCHIVE.out.matrix.collect().map { ['pre_filter_matrix.csv', it] }
            .mix(PARSE_ARCHIVE.out.dates.collect().map { ['pre_filter_dates.csv', it] })
    )
    matrix = merged.filter { it.name == 'pre_filter_matrix.csv' }.first()

    // 2.-4. NCBI steps in a chain: GEO -> SRA -> SRA recheck -> gold standard
    if (params.skip_geo) {
        geo_done = Channel.value('skipped')
    } else {
        geo_done = GEO_REFS()[0].map { 'done' }
    }
    if (params.skip_sra) {
        sra_done = geo_done
    } else {
        dl       = SRA_DOWNLOAD(matrix, geo_done)
        rechk    = SRA_RECHECK(matrix, dl)
        sra_done = SRA_CONCAT(rechk).map { 'done' }
    }
    if (params.skip_gold) {
        gold_done = sra_done
    } else {
        gold_done = GOLD_STANDARD(sra_done).map { 'done' }
    }

    // 5. notebooks and RUN_REPORT.md
    if (!params.skip_analyze) {
        ANALYZE(gold_done)
    }
}

workflow {
    if (params.mode == 'collect') {
        COLLECT()
    } else if (params.mode == 'analyze') {
        ANALYZE(Channel.value('existing data'))
    } else if (params.mode == 'reuse_check') {
        REUSE_CHECK()
    } else {
        error "Unknown --mode '${params.mode}' (use collect, analyze or reuse_check)"
    }
}
