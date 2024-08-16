import requests
from bs4 import BeautifulSoup
import os

base_url = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_noncomm/xml/"
download_dir = "/home/nicolaedrabcinski/research/lab/new_reuse/data/publications/oa_noncomm"

max_retries = 10
retry_delay = 10  # seconds

# Create the directory if it doesn't exist
os.makedirs(download_dir, exist_ok=True)

response = requests.get(base_url)
soup = BeautifulSoup(response.content, "html.parser")

# Find all links ending with .tar.gz
files = [a["href"] for a in soup.find_all("a") if a["href"].endswith(".tar.gz")]

def download_file(file_url, file_path):
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = requests.head(file_url)
            file_size = int(response.headers.get('Content-Length', 0))
            print(f"Expected size for {file_url}: {file_size} bytes")

            # Check if the file already exists and is fully downloaded
            if os.path.exists(file_path):
                if os.path.getsize(file_path) == file_size:
                    print(f"File {file_path} already downloaded.")
                    return
                else:
                    print(f"Incomplete file found. Resuming download for {file_url}...")

            with requests.get(file_url, stream=True) as r:
                r.raise_for_status()
                mode = 'ab' if os.path.exists(file_path) else 'wb'
                with open(file_path, mode) as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

            # Verify the file size after download
            if os.path.getsize(file_path) == file_size:
                print(f"Successfully downloaded {file_url}")
                return
            else:
                print(f"Size mismatch for {file_url}. Retrying...")
        except Exception as e:
            print(f"Error downloading {file_url}: {e}")
        
        retry_count += 1
        print(f"Retrying ({retry_count}/{max_retries})...")
        sleep(retry_delay)

    print(f"Failed to download {file_url} after {max_retries} attempts.")

for file in files:
    file_url = base_url + file
    file_path = os.path.join(download_dir, file)
    print(f"Downloading {file_url}...")
    download_file(file_url, file_path)

print("All files downloaded.")

