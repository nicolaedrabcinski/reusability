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
 *   then a text check on a stratified sample: pairs ──> full-text excerpts ──> local LLM (vLLM, GPU)
 *     ──> review_summary.md + review_todo.csv (only the pairs a person needs to look at)
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
    maxRetries 0
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
    maxRetries 0

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
    maxRetries 5

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
    publishDir "${params.outdir}/reuse_check", mode: 'copy'

    input:
    path geo_pairs
    path matrix

    output:
    path 'pairs.csv'

    script:
    """
    ${params.python} ${projectDir}/scripts/reuse_check/select_pairs.py \\
        --geo-pairs ${geo_pairs} --matrix ${matrix} --n ${params.text_check_n} --seed ${params.seed}
    """
}

process FETCH_CONTEXT {
    // One task (Europe PMC / NCBI are rate-limited). Raw XML is cached in outdir, reruns are cheap.
    publishDir "${params.outdir}/reuse_check", mode: 'copy'

    input:
    path pairs

    output:
    path 'contexts.jsonl'

    script:
    """
    ${params.python} ${projectDir}/scripts/reuse_check/fetch_context.py \\
        --pairs ${pairs} --cache-dir ${params.outdir}/reuse_check/xml_cache
    """
}

process LLM_CLASSIFY {
    // Local model on the GPU, inside the vLLM image (see nextflow.config, withName: LLM_CLASSIFY).
    publishDir "${params.outdir}/reuse_check", mode: 'copy'
    maxRetries 1

    input:
    path contexts
    path classifier   // the script itself: the container sees only the task directory

    output:
    path 'labels.jsonl'

    script:
    """
    python3 ${classifier} \\
        --contexts ${contexts} --model ${params.llm_model} --revision ${params.llm_revision} --out labels.jsonl
    """
}

process MAKE_REVIEW {
    publishDir "${params.outdir}/reuse_check", mode: 'copy'

    input:
    path pairs
    path contexts
    path labels

    output:
    path 'review_all.csv'
    path 'review_todo.csv'
    path 'review_summary.md'

    script:
    """
    ${params.python} ${projectDir}/scripts/reuse_check/make_review.py \\
        --pairs ${pairs} --contexts ${contexts} --labels ${labels} --seed ${params.seed}
    """
}

workflow REUSE_CHECK {
    matrix     = file("${params.outdir}/metadata_matrix_filtered.csv", checkIfExists: true)
    geo_series = file("${params.outdir}/geo_series.csv", checkIfExists: true)
    GEO_LINK_EVAL(matrix, geo_series, PMC_TO_PMID(matrix))

    if (!params.skip_text_check) {
        pairs    = SELECT_PAIRS(GEO_LINK_EVAL.out[0], matrix)
        contexts = FETCH_CONTEXT(pairs)
        labels   = LLM_CLASSIFY(contexts, file("${projectDir}/scripts/reuse_check/llm_classify.py"))
        MAKE_REVIEW(pairs, contexts, labels)
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
