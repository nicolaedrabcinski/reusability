/*
 * reuse_check: full-text excerpts and the local LLM, one chunk of pairs per task.
 * Included three times in main.nf (one alias per phase), so the phases run one after another.
 */

process FETCH_CONTEXT {
    // NCBI / Europe PMC are rate-limited: one chunk at a time (maxForks in nextflow.config).
    // It runs while the model works on the previous chunk. Raw XML is cached in outdir.
    tag "$chunk.baseName"

    input:
    path chunk
    val after   // the previous phase's texts are fetched

    output:
    tuple val(chunk.baseName), path("${chunk.baseName}.contexts.jsonl")

    script:
    """
    ${params.python} ${projectDir}/scripts/reuse_check/fetch_context.py \\
        --pairs ${chunk} --cache-dir ${params.outdir}/reuse_check/xml_cache --out ${chunk.baseName}.contexts.jsonl
    """

    stub:
    """
    sleep 2; echo '{}' > ${chunk.baseName}.contexts.jsonl
    """
}

process LLM_CLASSIFY {
    // Local model on the GPU, inside the vLLM image (see nextflow.config, withName: 'LLM_.*').
    tag "$name"
    publishDir "${params.reuse_check_dir}/${params.text_check_scope}/labels", mode: 'copy'

    input:
    tuple val(name), path(contexts)
    path classifier   // the script itself: the container sees only the task directory
    val after         // the previous phase's labels are done (one GPU)

    output:
    path "${name}.labels.jsonl"

    script:
    """
    python3 ${classifier} \\
        --contexts ${contexts} --model ${params.llm_model} --revision ${params.llm_revision} \\
        --out ${name}.labels.jsonl
    """

    stub:
    """
    sleep 5; touch ${name}.labels.jsonl
    """
}

