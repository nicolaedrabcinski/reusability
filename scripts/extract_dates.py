from concurrent.futures import ProcessPoolExecutor, as_completed
from lxml import etree
from tqdm import tqdm
import csv
import os


def _worker(pmc_path):
    pmc_path = pmc_path.strip()
    if not pmc_path:
        return None
    try:
        root = etree.parse(pmc_path, etree.XMLParser(recover=True)).getroot()
        accepted = root.find(".//date[@date-type='accepted']")
        if accepted is not None:
            y = accepted.findtext("year")
            m = accepted.findtext("month")
            d = accepted.findtext("day")
            if y and m and d:
                return (pmc_path, f"{y}/{m}/{d}")
    except Exception:
        pass
    return None


def process_xml_files(infile, output_file):
    with open(infile) as f:
        all_paths = [p.strip() for p in f if p.strip()]

    # Загрузить уже обработанные пути
    already_done = set()
    if os.path.exists(output_file):
        with open(output_file, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                already_done.add(row['File Name'])

    paths = [p for p in all_paths if p not in already_done]
    total = len(paths)
    workers = os.cpu_count() or 4
    print(f"Total XML: {len(all_paths):,} | Cached: {len(already_done):,} | To process: {total:,} | Workers: {workers}")

    if total == 0:
        print("All files already processed, skipping.")
        return

    # Append если файл уже есть, иначе write с заголовком
    file_exists = os.path.exists(output_file) and len(already_done) > 0
    mode = 'a' if file_exists else 'w'

    with open(output_file, mode, newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['File Name', 'Date'])
        if mode == 'w':
            writer.writeheader()

        chunk = 50_000
        for start in range(0, total, chunk):
            batch = paths[start:start + chunk]
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_worker, p): p for p in batch}
                with tqdm(total=len(batch),
                          desc=f"Files {start + len(already_done):,}–{min(start + chunk, total) + len(already_done):,}",
                          unit="xml") as pbar:
                    for future in as_completed(futures):
                        result = future.result()
                        if result:
                            writer.writerow({'File Name': result[0], 'Date': result[1]})
                        pbar.update(1)


if __name__ == "__main__":
    DATA_DIR = '../data'
    process_xml_files(f'{DATA_DIR}/pmc_paths.txt', f'{DATA_DIR}/pre_filter_dates.csv')
