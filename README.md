# Downloading publications
python ../scripts/utils/pubs/download_pubs_comm.py  
python ../scripts/utils/pubs/download_pubs_noncomm.py  
python ../scripts/utils/pubs/download_pubs_other.py  

# Unzip publications
python ../scripts/extract_pubs_tar.py  

# Parsing downloaded publications
python ../scripts/parsing_pubs.py  

# Downloading references
python ../scripts/utils/refs/download_refs.py  

# Downloading SRA
python ../scripts/utils/sra/download_sra.py  

# Postprocess downloaded data
python count_pubs.py  
python concat_pre_filter_matrices.py  
python generate_pmc_paths.py  
pytho extract_dates.py  

# Merge data scraped from the pmc publications onto reference data from SRA and GEO
cd notebooks  
jupyter notebook  

create.ipynb  
analyze.ipynb  
visualize.ipynb  



