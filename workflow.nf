#!/usr/bin/env nextflow

nextflow.enable.dsl=2

// Определяем путь к папке со скриптами
params.scripts_dir = file('./scripts').toAbsolutePath()

process download_pubs {
    output:
    path 'download_pubs_done.txt'

    """
    python ${params.scripts_dir}/download_pubs.py
    touch download_pubs_done.txt
    """
}

process download_geo_refs {
    input:
    path 'download_pubs_done.txt'
    output:
    path 'download_geo_refs_done.txt'

    """
    bash ${params.scripts_dir}/download_geo_refs_entrez.sh
    touch download_geo_refs_done.txt
    """
}

process download_sra_refs {
    input:
    path 'download_geo_refs_done.txt'
    output:
    path 'download_sra_refs_done.txt'

    """
    bash ${params.scripts_dir}/download_sra_refs.sh
    touch download_sra_refs_done.txt
    """
}

process download_geo_metadata {
    input:
    path all_files // Принимаем файлы с идентификаторами
    output:
    path 'metadata_output/*'  // Директория для сохранения метаданных

    script:
    """
    # Скачивание метаданных для всех GSE
    // bash ${params.scripts_dir}/download_geo_metadata.sh ${params.scripts_dir}/$input ${params.scripts_dir}/$output
    bash ${params.scripts_dir}/download_geo_metadata.sh $all_files ${params.scripts_dir}/metadata_output/
    """
}

workflow {
    download_pubs()
    download_geo_refs(download_pubs.out)
    download_sra_refs(download_geo_refs.out)
    download_geo_metadata(download_sra_refs.out)
}
