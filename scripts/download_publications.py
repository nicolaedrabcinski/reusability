from pathlib import Path
from tqdm import tqdm
from lxml import etree
from concurrent.futures import ProcessPoolExecutor, as_completed
import ftplib
import logging
import os
import re
import subprocess
import tarfile
import time
import json
import hashlib
import pandas as pd
import argparse

def setup_logging():
    """Logging setup for the script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("processing.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def setup_directories(base_dir):
    """Set up directories for script operations"""
    dirs = {
        'download_pub': base_dir / 'publications',
        'interim_pfm': base_dir / 'pre_filter_matrices',
        'cache': base_dir / 'cache',
        'state': base_dir / 'state'
    }
    
    for directory in dirs.values():
        directory.mkdir(exist_ok=True, parents=True)
    
    return dirs

class CacheManager:
    """Управление кешированием для скачивания и обработки файлов"""
    
    def __init__(self, cache_dir, state_dir):
        self.cache_dir = Path(cache_dir)
        self.state_dir = Path(state_dir)
        self.downloaded_files = self.load_downloaded_files()
        self.processed_files = self.load_processed_files()
        self.failed_files = self.load_failed_files()
    
    def get_file_hash(self, ftp_server, ftp_dir, filename):
        """Создает уникальный хеш для файла"""
        content = f"{ftp_server}{ftp_dir}{filename}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def load_downloaded_files(self):
        """Загружает список скачанных файлов"""
        downloaded_file = self.state_dir / "downloaded_files.json"
        if downloaded_file.exists():
            with open(downloaded_file, 'r') as f:
                return set(json.load(f))
        return set()
    
    def save_downloaded_files(self):
        """Сохраняет список скачанных файлов"""
        downloaded_file = self.state_dir / "downloaded_files.json"
        with open(downloaded_file, 'w') as f:
            json.dump(list(self.downloaded_files), f, indent=2)
    
    def load_processed_files(self):
        """Загружает список обработанных файлов"""
        processed_file = self.state_dir / "processed_files.json"
        if processed_file.exists():
            with open(processed_file, 'r') as f:
                return set(json.load(f))
        return set()
    
    def save_processed_files(self):
        """Сохраняет список обработанных файлов"""
        processed_file = self.state_dir / "processed_files.json"
        with open(processed_file, 'w') as f:
            json.dump(list(self.processed_files), f, indent=2)
    
    def load_failed_files(self):
        """Загружает список файлов с ошибками"""
        failed_file = self.state_dir / "failed_files.json"
        if failed_file.exists():
            with open(failed_file, 'r') as f:
                return set(json.load(f))
        return set()
    
    def save_failed_files(self):
        """Сохраняет список файлов с ошибками"""
        failed_file = self.state_dir / "failed_files.json"
        with open(failed_file, 'w') as f:
            json.dump(list(self.failed_files), f, indent=2)
    
    def is_file_downloaded(self, ftp_server, ftp_dir, filename):
        """Проверяет, скачан ли файл"""
        file_hash = self.get_file_hash(ftp_server, ftp_dir, filename)
        return file_hash in self.downloaded_files
    
    def is_file_processed(self, ftp_server, ftp_dir, filename):
        """Проверяет, обработан ли файл"""
        file_hash = self.get_file_hash(ftp_server, ftp_dir, filename)
        return file_hash in self.processed_files
    
    def is_file_failed(self, ftp_server, ftp_dir, filename):
        """Проверяет, провалилась ли обработка файла"""
        file_hash = self.get_file_hash(ftp_server, ftp_dir, filename)
        return file_hash in self.failed_files
    
    def mark_file_downloaded(self, ftp_server, ftp_dir, filename):
        """Отмечает файл как скачанный"""
        file_hash = self.get_file_hash(ftp_server, ftp_dir, filename)
        self.downloaded_files.add(file_hash)
        self.save_downloaded_files()
    
    def mark_file_processed(self, ftp_server, ftp_dir, filename):
        """Отмечает файл как обработанный"""
        file_hash = self.get_file_hash(ftp_server, ftp_dir, filename)
        self.processed_files.add(file_hash)
        self.save_processed_files()
    
    def mark_file_failed(self, ftp_server, ftp_dir, filename):
        """Отмечает файл как провалившийся"""
        file_hash = self.get_file_hash(ftp_server, ftp_dir, filename)
        self.failed_files.add(file_hash)
        self.save_failed_files()
    
    def get_cache_stats(self):
        """Возвращает статистику кеша"""
        return {
            'downloaded': len(self.downloaded_files),
            'processed': len(self.processed_files),
            'failed': len(self.failed_files)
        }

ACCESSION_PATTERNS = [
    # SRA — Run/Study/Experiment/Sample/Submission/Archive (NCBI/EBI/DDBJ)
    r'\b[SDE]RR[0-9]{6,7}\b',  # Run        (SRR / ERR / DRR)
    r'\b[SDE]RP[0-9]{6,7}\b',  # Study      (SRP / ERP / DRP)
    r'\b[SDE]RX[0-9]{6,7}\b',  # Experiment (SRX / ERX / DRX)
    r'\b[SDE]RS[0-9]{6,7}\b',  # Sample     (SRS / ERS / DRS)
    r'\b[SDE]RZ[0-9]{6,7}\b',  # Submission (SRZ / ERZ / DRZ)
    r'\b[SDE]RA[0-9]{6,7}\b',  # Archive    (SRA / ERA / DRA)
    r'\bPRJNA[0-9]{6,7}\b',    # BioProject NCBI
    # GEO — only officially documented accession types
    r'\bGDS[0-9]{1,6}\b',      # GEO DataSet
    r'\bGSE[0-9]{1,6}\b',      # GEO Series
    r'\bGPL[0-9]{1,6}\b',      # GEO Platform
]

# FTP servers and directories
FTP_SERVERS = [
    ('ftp.ncbi.nlm.nih.gov', '/pub/pmc/oa_bulk/oa_comm/xml/'),
    ('ftp.ncbi.nlm.nih.gov', '/pub/pmc/oa_bulk/oa_noncomm/xml/'),
    ('ftp.ncbi.nlm.nih.gov', '/pub/pmc/oa_bulk/oa_other/xml/')
]

def get_tar_gz_filenames(ftp_server, ftp_dir, logger):
    """Retrieve a list of .tar.gz files from an FTP directory with error handling."""
    try:
        with ftplib.FTP(ftp_server, timeout=30) as ftp:
            ftp.login()
            ftp.cwd(ftp_dir)
            file_list = ftp.nlst()
            tar_gz_files = [file for file in file_list if file.endswith('.tar.gz')]
            logger.info(f"Found {len(tar_gz_files)} .tar.gz files in {ftp_server}{ftp_dir}")
            return tar_gz_files
    except ftplib.all_errors as e:
        logger.error(f"Error connecting to FTP server {ftp_server}: {e}")
        return []

def download_file_from_ftp(ftp_server, ftp_dir, filename, archive_path, cache_manager, logger):
    """Download a file from an FTP server with caching."""
    # Проверяем, скачан ли файл уже
    if cache_manager.is_file_downloaded(ftp_server, ftp_dir, filename) and archive_path.exists():
        logger.info(f"File already downloaded (cached): {archive_path.name}")
        return True
    
    # Проверяем, существует ли файл на диске (без записи в кеше)
    if archive_path.exists():
        file_size = archive_path.stat().st_size
        if file_size > 0:
            logger.info(f"File exists on disk, marking as downloaded: {archive_path.name}")
            cache_manager.mark_file_downloaded(ftp_server, ftp_dir, filename)
            return True
        else:
            logger.warning(f"Empty file found, re-downloading: {archive_path.name}")
            archive_path.unlink()
    
    try:
        cmd = ['wget', f'ftp://{ftp_server}{ftp_dir}/{filename}', '-O', str(archive_path), '--continue', '--quiet']
        subprocess.run(cmd, check=True)
        
        # Проверяем, что файл скачался корректно
        if archive_path.exists() and archive_path.stat().st_size > 0:
            logger.info(f"File downloaded: {archive_path.name}")
            cache_manager.mark_file_downloaded(ftp_server, ftp_dir, filename)
            return True
        else:
            logger.error(f"Downloaded file is empty or missing: {filename}")
            return False
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Error downloading {filename}: {e}")
        return False

def extract_tar_gz(archive_path, extract_dir, logger):
    """File-by-file extraction."""
    try:
        with tarfile.open(archive_path, "r:gz") as tar:
            members = [m for m in tar.getmembers() if m.isfile()]
            for member in tqdm(members, desc=f"Extracting {archive_path.name}"):
                try:
                    tar.extract(member, path=extract_dir)
                except Exception as e:
                    logger.warning(f"Failed to extract file {member.name}: {e}")
        logger.info(f"Archive successfully extracted {archive_path}")
        return True
    except (tarfile.TarError, EOFError, OSError) as e:
        logger.error(f"Error while extracting {archive_path}: {e}")
        logger.info(f"Deleting corrupted archive: {archive_path}")
        archive_path.unlink(missing_ok=True)
        return False

def _parse_xml_worker(xml_path_str):
    """Top-level worker for ProcessPoolExecutor: parse one XML file, return rows."""
    xml_path = Path(xml_path_str)
    pmc_id = xml_path.stem
    journal_name = None
    try:
        for _, elem in etree.iterparse(xml_path_str, events=('end',), tag='journal-meta', recover=True):
            journal_id_elem = elem.find('.//journal-id[@journal-id-type="nlm-ta"]')
            if journal_id_elem is not None and journal_id_elem.text:
                journal_name = journal_id_elem.text.replace(',', ' ').strip()
            elem.clear()
            break
    except Exception:
        pass

    accessions = set()
    try:
        content = xml_path.read_text(encoding='utf-8', errors='ignore')
        for pattern in ACCESSION_PATTERNS:
            accessions.update(re.findall(pattern, content))
    except Exception:
        pass

    if journal_name and accessions:
        return [{'journal_name': journal_name, 'pmc_id': pmc_id, 'accession': acc} for acc in accessions]
    return []


def process_xml_file(xml_path, logger):
    """Extract journal name and accession numbers from a single XML file."""
    rows = _parse_xml_worker(str(xml_path))
    if rows:
        return rows[0]['journal_name'], rows[0]['pmc_id'], [r['accession'] for r in rows]
    return None, xml_path.stem, []

def process_file(file_path, dirs, logger):
    """Process all XML files in directory: extract journal name and accessions per file."""
    logger.info(f"Processing directory: {file_path}")

    archive_name = Path(file_path).name
    output_path = dirs['interim_pfm'] / f"{archive_name}_pre_filter_matrix.csv"

    xml_files = [str(p) for p in Path(file_path).glob('**/*.xml')]
    n = len(xml_files)
    workers = os.cpu_count() or 4
    logger.info(f"Found {n} XML files to parse — using {workers} workers")

    rows = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_parse_xml_worker, f): f for f in xml_files}
        with tqdm(total=n, desc=f"Parsing {archive_name}", unit="xml") as pbar:
            for i, future in enumerate(as_completed(futures), 1):
                rows.extend(future.result())
                pbar.update(1)
                if i % 50_000 == 0:
                    logger.info(f"  {i}/{n} XML parsed, {len(rows)} rows so far")

    pd.DataFrame(rows, columns=['journal_name', 'pmc_id', 'accession']).to_csv(output_path, index=False)
    logger.info(f"Wrote {len(rows)} rows to {output_path}")
    return True

def process_archive(archive_path, extract_dir, dirs, logger):
    """Process an archive: extract and handle its contents."""
    logger.info(f"Processing archive: {archive_path}")
    
    try:
        if not os.path.exists(archive_path):
            logger.error(f"Archive {archive_path} does not exist.")
            return False
        
        if extract_tar_gz(archive_path, extract_dir, logger):
            return process_file(extract_dir, dirs, logger)
        else:
            logger.error(f"Failed to extract {archive_path}")
            return False
    except Exception as e:
        logger.error(f"An error occurred while processing archive {archive_path}: {e}")
        return False

def download_and_process_file(args):
    """Download and process a single file."""
    ftp_server, ftp_dir, filename, dirs, cache_manager, logger, retries = args
    
    archive_path = dirs['download_pub'] / filename
    extract_dir = Path(str(archive_path).replace('.tar.gz', ''))
    
    # Проверяем, обработан ли файл полностью
    if cache_manager.is_file_processed(ftp_server, ftp_dir, filename):
        logger.info(f"File {filename} already fully processed (cached), skipping.")
        return True, filename
    
    # Проверяем, не провалился ли файл ранее (опционально можно пропустить)
    if cache_manager.is_file_failed(ftp_server, ftp_dir, filename):
        logger.info(f"File {filename} previously failed, skipping (use --retry-failed to retry).")
        return False, filename
    
    # Проверяем результирующий файл
    archive_name = filename.replace('.tar.gz', '')
    pre_filter_matrix = dirs['interim_pfm'] / f"{archive_name}_pre_filter_matrix.csv"
    if os.path.exists(pre_filter_matrix) and os.path.getsize(pre_filter_matrix) > 0:
        logger.info(f"Output file exists for {filename}, marking as processed.")
        cache_manager.mark_file_processed(ftp_server, ftp_dir, filename)
        return True, filename
    
    os.makedirs(extract_dir, exist_ok=True)
    
    for attempt in range(retries):
        try:
            if download_file_from_ftp(ftp_server, ftp_dir, filename, archive_path, cache_manager, logger):
                if process_archive(archive_path, extract_dir, dirs, logger):
                    cache_manager.mark_file_processed(ftp_server, ftp_dir, filename)
                    return True, filename
            
            if attempt < retries - 1:
                logger.info(f"Retrying download of {filename} (attempt {attempt + 2}/{retries})")
                time.sleep(5 * (attempt + 1))
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            if attempt < retries - 1:
                logger.info(f"Retrying download of {filename} (attempt {attempt + 2}/{retries})")
                time.sleep(5 * (attempt + 1))
    
    # Отмечаем как неудачный
    cache_manager.mark_file_failed(ftp_server, ftp_dir, filename)
    return False, filename

def download_and_process_ftp_files(file_list, ftp_server, ftp_dir, dirs, cache_manager, logger, retries=3):
    """Download and process files from an FTP server sequentially."""
    failed_downloads = []
    successful_downloads = []
    skipped_downloads = []

    for filename in tqdm(file_list, desc=f'Processing files from {ftp_server}{ftp_dir}'):
        # Быстрая проверка кеша
        if cache_manager.is_file_processed(ftp_server, ftp_dir, filename):
            skipped_downloads.append(filename)
            continue
            
        success, name = download_and_process_file((ftp_server, ftp_dir, filename, dirs, cache_manager, logger, retries))
        if success:
            successful_downloads.append(name)
        else:
            failed_downloads.append(name)

    logger.info(f"Successfully processed: {len(successful_downloads)} files")
    logger.info(f"Skipped (cached): {len(skipped_downloads)} files")
    if failed_downloads:
        logger.warning(f"Failed to process: {len(failed_downloads)} files")

    return successful_downloads, failed_downloads, skipped_downloads

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Download and process publication data from NCBI FTP.')
    parser.add_argument('--data-dir', type=str, default='../data',
                        help='Base directory for storing data (default: ./data)')
    parser.add_argument('--retries', type=int, default=3, 
                        help='Number of retry attempts on failure (default: 3)')
    parser.add_argument('--limit', type=int, default=None, 
                        help='Limit the number of files to download for testing (default: no limit)')
    parser.add_argument('--retry-failed', action='store_true',
                        help='Retry previously failed files')
    parser.add_argument('--clear-cache', action='store_true',
                        help='Clear all cache and start fresh')
    parser.add_argument('--show-stats', action='store_true',
                        help='Show cache statistics and exit')
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    logger = setup_logging()
    
    base_dir = Path(args.data_dir)
    logger.info(f"Base data directory: {base_dir}")
    
    dirs = setup_directories(base_dir)
    cache_manager = CacheManager(dirs['cache'], dirs['state'])
    
    # Показать статистику и выйти
    if args.show_stats:
        stats = cache_manager.get_cache_stats()
        logger.info("=== CACHE STATISTICS ===")
        logger.info(f"Downloaded files: {stats['downloaded']}")
        logger.info(f"Processed files: {stats['processed']}")
        logger.info(f"Failed files: {stats['failed']}")
        return
    
    # Сбросить ранее упавшие файлы — повторить их обработку
    if args.retry_failed:
        logger.info(f"Clearing {len(cache_manager.failed_files)} failed files from cache for retry.")
        cache_manager.failed_files.clear()
        cache_manager.save_failed_files()

    # Очистить кеш
    if args.clear_cache:
        logger.info("Clearing cache...")
        cache_manager.downloaded_files.clear()
        cache_manager.processed_files.clear()
        cache_manager.failed_files.clear()
        cache_manager.save_downloaded_files()
        cache_manager.save_processed_files()
        cache_manager.save_failed_files()
        logger.info("Cache cleared.")
    
    logger.info("Starting the download and processing workflow.")
    
    # Показать начальную статистику
    stats = cache_manager.get_cache_stats()
    logger.info(f"Cache stats - Downloaded: {stats['downloaded']}, Processed: {stats['processed']}, Failed: {stats['failed']}")
    
    total_files = 0
    total_success = 0
    total_failed = 0
    total_skipped = 0
    
    # Process each FTP server
    for ftp_server, ftp_dir in FTP_SERVERS:
        logger.info(f"Retrieving file list from {ftp_server}{ftp_dir}")
        
        tar_gz_files = get_tar_gz_filenames(ftp_server, ftp_dir, logger)
        
        if args.limit and len(tar_gz_files) > args.limit:
            logger.info(f"Limiting file list to {args.limit} (from {len(tar_gz_files)})")
            tar_gz_files = tar_gz_files[:args.limit]
        
        total_files += len(tar_gz_files)
        
        if not tar_gz_files:
            logger.warning(f"No files found on {ftp_server}{ftp_dir}")
            continue
        
        successful, failed, skipped = download_and_process_ftp_files(
            tar_gz_files, ftp_server, ftp_dir, dirs, cache_manager, logger, 
            retries=args.retries
        )

        total_success += len(successful)
        total_failed += len(failed)
        total_skipped += len(skipped)
    
    # Final report
    logger.info("=" * 50)
    logger.info("Final report:")
    logger.info(f"Total files: {total_files}")
    logger.info(f"Successfully processed: {total_success}")
    logger.info(f"Skipped (cached): {total_skipped}")
    logger.info(f"Failed to process: {total_failed}")
    
    final_stats = cache_manager.get_cache_stats()
    logger.info(f"Final cache stats - Downloaded: {final_stats['downloaded']}, Processed: {final_stats['processed']}, Failed: {final_stats['failed']}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
