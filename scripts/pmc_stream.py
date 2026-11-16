"""Stream a Europe PMC open-access .xml.gz file article by article."""
import gzip

from lxml import etree

from accessions import find_accessions


def _pmcid(article):
    for el in article.iterfind('.//article-meta/article-id'):
        if el.get('pub-id-type') in ('pmcid', 'pmc') and el.text:
            val = el.text.strip()
            return val if val.upper().startswith('PMC') else f'PMC{val}'
    return None


def _journal(article):
    el = article.find('.//journal-meta/journal-id[@journal-id-type="nlm-ta"]')
    if el is not None and el.text:
        return el.text.replace(',', ' ').strip()
    return None


def _accepted_date(article):
    d = article.find(".//date[@date-type='accepted']")
    if d is None:
        return None
    y, m, day = d.findtext('year'), d.findtext('month'), d.findtext('day')
    if not (y and m and day):
        return None
    try:
        return f"{int(y):04d}/{int(m):02d}/{int(day):02d}"
    except ValueError:
        return None


def iter_articles(path):
    """Yield one dict per <article>: pmc_id, journal, accessions (sorted list), accepted.

    Only articles that mention at least one accession get their journal/date looked up;
    for the others the fields are None and accessions is empty.
    Raises OSError/EOFError/XMLSyntaxError on a truncated or corrupt file.
    """
    with gzip.open(path, 'rb') as fh:
        for _, elem in etree.iterparse(fh, events=('end',), tag='article',
                                       recover=True, huge_tree=True):
            text = etree.tostring(elem, encoding='utf-8').decode('utf-8', errors='ignore')
            accessions = sorted(find_accessions(text))
            rec = {'pmc_id': None, 'journal': None, 'accessions': accessions, 'accepted': None}
            if accessions:
                rec.update(pmc_id=_pmcid(elem), journal=_journal(elem), accepted=_accepted_date(elem))
            yield rec
            elem.clear()  # free memory
            while elem.getprevious() is not None:
                del elem.getparent()[0]
