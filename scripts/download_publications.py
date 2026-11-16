from pathlib import Path
from tqdm import tqdm
from lxml import etree
from accessions import find_accessions
import ftplib
import logging
import subprocess
import gzip
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

# FTP servers and directories (Europe PMC mirror of the PMC open-access set;
# each file is one .xml.gz holding many concatenated <article> elements)
FTP_SERVERS = [
    ('ftp.ebi.ac.uk', '/pub/databases/pmc/oa/')
]

def get_xml_gz_filenames(ftp_server, ftp_dir, logger):
    """Retrieve a list of .xml.gz files from an FTP directory with error handling."""
    try:
        with ftplib.FTP(ftp_server, timeout=30) as ftp:
            ftp.login()
            ftp.cwd(ftp_dir)
            file_list = ftp.nlst()
            xml_gz_files = sorted(f for f in file_list if f.endswith('.xml.gz'))
            logger.info(f"Found {len(xml_gz_files)} .xml.gz files in {ftp_server}{ftp_dir}")
            return xml_gz_files
    except ftplib.all_errors as e:
        logger.error(f"Error connecting to FTP server {ftp_server}: {e}")
        return []

def download_file_from_ftp(ftp_server, ftp_dir, filename, archive_path, cache_manager, logger):
    """Download a file from an FTP server with caching."""
    if archive_path.is_file() and archive_path.stat().st_size > 0:
        if not cache_manager.is_file_downloaded(ftp_server, ftp_dir, filename):
            cache_manager.mark_file_downloaded(ftp_server, ftp_dir, filename)
        logger.info(f"File already on disk: {archive_path.name}")
        return True
    if archive_path.is_dir():
        logger.error(f"{archive_path} is a directory, expected a file. Remove it and retry.")
        return False
    archive_path.unlink(missing_ok=True)

    try:
        url = f"ftp://{ftp_server}/{ftp_dir.strip('/')}/{filename}"
        subprocess.run(['wget', url, '-O', str(archive_path), '--continue', '--quiet'], check=True)
        if archive_path.is_file() and archive_path.stat().st_size > 0:
            logger.info(f"File downloaded: {archive_path.name}")
            cache_manager.mark_file_downloaded(ftp_server, ftp_dir, filename)
            return True
        logger.error(f"Downloaded file is empty or missing: {filename}")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Error downloading {filename}: {e}")
        return False

def _article_pmcid(article):
    """Return the PMC id (e.g. 'PMC10000002') of an <article> element, or None."""
    for el in article.iterfind('.//article-meta/article-id'):
        if el.get('pub-id-type') in ('pmcid', 'pmc') and el.text:
            val = el.text.strip()
            return val if val.upper().startswith('PMC') else f'PMC{val}'
    return None

def _article_journal(article):
    el = article.find('.//journal-meta/journal-id[@journal-id-type="nlm-ta"]')
    if el is not None and el.text:
        return el.text.replace(',', ' ').strip()
    return None

def process_archive(archive_path, dirs, logger):
    """Stream one .xml.gz archive article by article.

    Writes a pre-filter matrix CSV (journal_name, pmc_id, accession) and saves the XML of
    every article that mentions at least one accession to publications/<archive>/<PMCID>.xml
    so later steps (dates) can read it.
    """
    archive_name = archive_path.name[:-len('.xml.gz')]
    output_path = dirs['interim_pfm'] / f"{archive_name}_pre_filter_matrix.csv"
    tmp_path = output_path.with_suffix('.csv.tmp')
    article_dir = dirs['download_pub'] / archive_name
    logger.info(f"Processing archive: {archive_path}")

    rows = []
    n_articles = 0
    try:
        article_dir.mkdir(exist_ok=True, parents=True)
        with gzip.open(archive_path, 'rb') as fh:
            for _, elem in tqdm(etree.iterparse(fh, events=('end',), tag='article',
                                                recover=True, huge_tree=True),
                                desc=f"Parsing {archive_name}", unit="article"):
                n_articles += 1
                xml_bytes = etree.tostring(elem, encoding='utf-8')
                accessions = find_accessions(xml_bytes.decode('utf-8', errors='ignore'))
                if accessions:
                    pmc_id = _article_pmcid(elem)
                    journal = _article_journal(elem)
                    if pmc_id and journal:
                        rows.extend({'journal_name': journal, 'pmc_id': pmc_id, 'accession': a}
                                    for a in sorted(accessions))
                        (article_dir / f"{pmc_id}.xml").write_bytes(xml_bytes)
                # free memory
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
    except (OSError, EOFError, etree.XMLSyntaxError) as e:
        logger.error(f"Error while parsing {archive_path}: {e}")
        tmp_path.unlink(missing_ok=True)
        archive_path.unlink(missing_ok=True)  # likely truncated; force re-download
        return False

    if n_articles == 0:
        logger.error(f"No <article> elements found in {archive_path}")
        archive_path.unlink(missing_ok=True)
        return False

    pd.DataFrame(rows, columns=['journal_name', 'pmc_id', 'accession']).to_csv(tmp_path, index=False)
    tmp_path.replace(output_path)  # atomic: a partial CSV is never mistaken for a finished one
    logger.info(f"{n_articles} articles parsed, wrote {len(rows)} rows to {output_path}")
    return True

def download_and_process_file(args):
    """Download and process a single file."""
    ftp_server, ftp_dir, filename, dirs, cache_manager, logger, retries, keep_archives = args

    archive_path = dirs['download_pub'] / filename

    if cache_manager.is_file_processed(ftp_server, ftp_dir, filename):
        logger.info(f"File {filename} already fully processed (cached), skipping.")
        return True, filename

    if cache_manager.is_file_failed(ftp_server, ftp_dir, filename):
        logger.info(f"File {filename} previously failed, skipping (use --retry-failed to retry).")
        return False, filename

    archive_name = filename[:-len('.xml.gz')]
    pre_filter_matrix = dirs['interim_pfm'] / f"{archive_name}_pre_filter_matrix.csv"
    if pre_filter_matrix.exists() and pre_filter_matrix.stat().st_size > 0:
        logger.info(f"Output file exists for {filename}, marking as processed.")
        cache_manager.mark_file_processed(ftp_server, ftp_dir, filename)
        return True, filename

    for attempt in range(retries):
        try:
            if download_file_from_ftp(ftp_server, ftp_dir, filename, archive_path, cache_manager, logger):
                if process_archive(archive_path, dirs, logger):
                    cache_manager.mark_file_processed(ftp_server, ftp_dir, filename)
                    if not keep_archives:
                        archive_path.unlink(missing_ok=True)
                    return True, filename
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
        if attempt < retries - 1:
            logger.info(f"Retrying {filename} (attempt {attempt + 2}/{retries})")
            time.sleep(5 * (attempt + 1))

    cache_manager.mark_file_failed(ftp_server, ftp_dir, filename)
    return False, filename

def download_and_process_ftp_files(file_list, ftp_server, ftp_dir, dirs, cache_manager, logger, retries=3, keep_archives=False):
    """Download and process files from an FTP server sequentially."""
    failed_downloads = []
    successful_downloads = []
    skipped_downloads = []

    for filename in tqdm(file_list, desc=f'Processing files from {ftp_server}{ftp_dir}'):
        # Быстрая проверка кеша
        if cache_manager.is_file_processed(ftp_server, ftp_dir, filename):
            skipped_downloads.append(filename)
            continue
            
        success, name = download_and_process_file((ftp_server, ftp_dir, filename, dirs, cache_manager, logger, retries, keep_archives))
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
    parser = argparse.ArgumentParser(description='Download and process publication data from the Europe PMC FTP.')
    parser.add_argument('--data-dir', type=str, default='../data',
                        help='Base directory for storing data (default: ./data)')
    parser.add_argument('--retries', type=int, default=3, 
                        help='Number of retry attempts on failure (default: 3)')
    parser.add_argument('--limit', type=int, default=None, 
                        help='Limit the number of files to download for testing (default: no limit)')
    parser.add_argument('--keep-archives', action='store_true',
                        help='Keep downloaded .xml.gz archives after processing (default: delete)')
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
        
        xml_gz_files = get_xml_gz_filenames(ftp_server, ftp_dir, logger)
        
        if args.limit and len(xml_gz_files) > args.limit:
            logger.info(f"Limiting file list to {args.limit} (from {len(xml_gz_files)})")
            xml_gz_files = xml_gz_files[:args.limit]
        
        total_files += len(xml_gz_files)
        
        if not xml_gz_files:
            logger.warning(f"No files found on {ftp_server}{ftp_dir}")
            continue
        
        successful, failed, skipped = download_and_process_ftp_files(
            xml_gz_files, ftp_server, ftp_dir, dirs, cache_manager, logger, 
            retries=args.retries, keep_archives=args.keep_archives
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
