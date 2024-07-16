import ftplib
import os
import re
import shutil
import subprocess
import tarfile
import time

from tqdm import tqdm

# Define directories
download_pub_dir = '/home/nicolaedrabcinski/research/lab/new_reuse/data/publications'
interim_rp_data = '/home/nicolaedrabcinski/research/lab/new_reuse/data/raw_pub_data'
interim_jn_data = '/home/nicolaedrabcinski/research/lab/new_reuse/data/journal_names'
interim_acc_data = '/home/nicolaedrabcinski/research/lab/new_reuse/data/accessions'
interim_pfm_data = '/home/nicolaedrabcinski/research/lab/new_reuse/data/pre_filter_matrices'

# Functions
def get_tar_gz_filenames(ftp_server, ftp_dir):
    """Get the list of .tar.gz files from an FTP directory."""
    with ftplib.FTP(ftp_server) as ftp:
        ftp.login()
        ftp.cwd(ftp_dir)
        file_list = ftp.nlst()
        tar_gz_files = [file for file in file_list if file.endswith('.tar.gz')]
    return tar_gz_files

def download_file_from_ftp(ftp_server, ftp_dir, filename, archive_path):
    """Download a file from an FTP server."""
    try:
        subprocess.run(['wget', f'ftp://{ftp_server}/{ftp_dir}/{filename}', '-O', archive_path, '--continue'], check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error downloading {filename}: {e}")

def extract_tar_gz(archive_path, extract_dir):
    """Extract a .tar.gz archive to a specified directory."""
    try:
        with tarfile.open(archive_path, 'r:gz') as tar:
            tar.extractall(path=extract_dir)
    except tarfile.TarError as e:
        raise RuntimeError(f"Error extracting {archive_path}: {e}")

def search_journal_xml(directory_path):
    """Search for journal names in XML files."""
    regex_pattern = r'<journal-meta>.*?<journal-id journal-id-type="nlm-ta">(.*?)</journal-id>'
    matches = []

    for root, dirs, files in os.walk(directory_path):
        for file_name in files:
            if file_name.endswith('.xml'):
                file_path = os.path.join(root, file_name)
                with open(file_path, 'r') as file:
                    content = file.read()
                    matches.extend(re.findall(regex_pattern, content))

    matches = [match.replace(',', ' ') for match in matches]
    return matches

def generate_tmp_file_paths(file_path):
    """Generate temporary file paths based on the provided file path."""
    base_file_name = os.path.basename(file_path)
    tmp_raw_pub_data = os.path.join(interim_rp_data, base_file_name + "_raw_pub_data.txt")
    tmp_journal_names = os.path.join(interim_jn_data, base_file_name + "_journal_names.txt")
    tmp_pre_filter_matrix = os.path.join(interim_pfm_data, base_file_name + "_pre_filter_matrix.csv")
    return tmp_raw_pub_data, tmp_journal_names, tmp_pre_filter_matrix

def extract_accession_numbers(file_path, tmp_raw_pub_data):
    """Extract accession numbers from files and save to a temporary file."""
    grep_command = (
        f"grep -o -r -E -H "
        f"-e '[SDE]R[APXRSZ][0-9]{{6,7}}' "
        f"-e 'PRJNA[0-9]{{6,7}}' "
        f"-e 'PRJD[0-9]{{6,7}}' "
        f"-e 'PRJEB[0-9]{{6,7}}' "
        f"-e 'GDS[0-9]{{1,6}}' "
        f"-e 'GSE[0-9]{{1,6}}' "
        f"-e 'GPL[0-9]{{1,6}}' "
        f"-e 'GSM[0-9]{{1,6}}' "
        f"{file_path}"
    )
    os.system(f"{grep_command} > {tmp_raw_pub_data}")

def format_raw_data(tmp_raw_pub_data):
    """Format raw data into CSV format."""
    with open(tmp_raw_pub_data, 'r') as file:
        raw_data = file.readlines()
    pmc_accs_data = [
        line.strip().split("/")[-1].replace('.xml', '').rpartition(":")[0] + "," +
        line.strip().split("/")[-1].replace('.xml', '').rpartition(":")[2]
        for line in raw_data
    ]
    with open(os.path.join(interim_acc_data, os.path.basename(tmp_raw_pub_data) + "_accessions.csv"), 'w') as file:
        file.write("pmc_id,accession\n")
        file.write("\n".join(pmc_accs_data))

def combine_journal_and_accession(tmp_journal_names, tmp_pre_filter_matrix, tmp_raw_pub_data):
    """Combine journal names and accession numbers into a single CSV file."""
    with open(tmp_pre_filter_matrix, 'w') as file:
        file.write('journal_name,pmc_id,accession\n')
        with open(tmp_journal_names, 'r') as journal_file:
            journal_names = journal_file.readlines()[1:]
        with open(os.path.join(interim_acc_data, os.path.basename(tmp_raw_pub_data) + "_accessions.csv"), 'r') as pmc_file:
            pmc_accs = pmc_file.readlines()[1:]
        for journal, pmc_acc in zip(journal_names, pmc_accs):
            file.write(journal.strip() + "," + pmc_acc)

def process_file(file_path):
    """Process a single file: extract accession numbers and combine with journal names."""
    tmp_raw_pub_data, tmp_journal_names, tmp_pre_filter_matrix = generate_tmp_file_paths(file_path)
    extract_accession_numbers(file_path, tmp_raw_pub_data)
    journal_names = search_journal_xml(file_path)
    with open(tmp_journal_names, 'w') as file:
        file.write("journal_name\n")
        file.write("\n".join(journal_names))
    format_raw_data(tmp_raw_pub_data)
    combine_journal_and_accession(tmp_journal_names, tmp_pre_filter_matrix, tmp_raw_pub_data)

def process_archive(archive_path, extract_dir):
    """Process an archive: extract and process the contents."""
    try:
        extract_tar_gz(archive_path, extract_dir)
        process_file(extract_dir)
    except Exception as e:
        tqdm.write(f"An error occurred while processing {archive_path}: {e}")

def download_and_process_ftp_files(file_list, ftp_server, ftp_dir, retries=3):
    """Download and process files from an FTP server."""
    failed_downloads = []

    for filename in tqdm(file_list, desc='Processing files'):
        archive_path = os.path.join(download_pub_dir, filename)
        extract_dir = os.path.splitext(os.path.splitext(archive_path)[0])[0]
        os.makedirs(extract_dir, exist_ok=True)

        for attempt in range(retries):
            try:
                download_file_from_ftp(ftp_server, ftp_dir, filename, archive_path)
                process_archive(archive_path, extract_dir)
                break
            except Exception as e:
                tqdm.write(f"Error downloading or processing {filename}: {e}")
                if attempt == retries - 1:
                    failed_downloads.append(filename)
                    tqdm.write(f"Failed to download {filename} after {retries} attempts.")
                else:
                    tqdm.write(f"Retrying download for {filename}...")
                    time.sleep(2)  

    if failed_downloads:
        with open("failed_downloads.txt", "w") as file:
            for filename in failed_downloads:
                file.write(filename + "\n")
        tqdm.write(f"{len(failed_downloads)} files failed to download. See 'failed_downloads.txt' for details.")

def get_all_tar_gz_files(ftp_servers):
    tar_gz_files_list = []
    for ftp_server, ftp_dir in ftp_servers:
        tar_gz_files = get_tar_gz_filenames(ftp_server, ftp_dir)
        tar_gz_files_list.append(tar_gz_files)
    return tar_gz_files_list

ftp_servers = [
    ('ftp.ncbi.nlm.nih.gov', '/pub/pmc/oa_bulk/oa_comm/xml/'),
    ('ftp.ncbi.nlm.nih.gov', '/pub/pmc/oa_bulk/oa_noncomm/xml/'),
    ('ftp.ncbi.nlm.nih.gov', '/pub/pmc/oa_bulk/oa_other/xml/')
]

os.makedirs(download_pub_dir, exist_ok=True)

tar_gz_files_list = get_all_tar_gz_files(ftp_servers)

for (ftp_server, ftp_dir), tar_gz_files in zip(ftp_servers, tar_gz_files_list):
    download_and_process_ftp_files(tar_gz_files, ftp_server, ftp_dir)
