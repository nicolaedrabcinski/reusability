"""Shared accession-number pattern used by the publication parsing step."""
import re

# SRA Run/Study/Experiment/Sample/Submission/Archive (NCBI/EBI/DDBJ), BioProject (NCBI),
# and GEO DataSet/Series/Platform. Modern SRA accessions have up to 9 digits
# (e.g. SRR12345678), so the digit range must not be capped at 7.
ACCESSION_RE = re.compile(
    r'\b(?:[SDE]R[RPXSZA][0-9]{6,9}|PRJNA[0-9]{6,9}|GDS[0-9]{1,7}|GSE[0-9]{1,7}|GPL[0-9]{1,7})\b'
)


def find_accessions(text):
    """Return the set of accession numbers mentioned in text."""
    return set(ACCESSION_RE.findall(text))
