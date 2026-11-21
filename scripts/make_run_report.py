"""Build RUN_REPORT.md from data/ and RUN_LOG.md (read-only on data; never touches secrets).

Usage: python scripts/make_run_report.py [--data-dir data] [--out RUN_REPORT.md]
Every section is guarded: a missing file is reported as missing, not as an error.
"""
import argparse
import glob
import os
import re
import subprocess
import sys

import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument('--data-dir', default='data')
ap.add_argument('--out', default='RUN_REPORT.md')
ap.add_argument('--log', default='RUN_LOG.md')
args = ap.parse_args()
D = args.data_dir
out = []


def p(s=''):
    out.append(s)


def fmt_size(n):
    for u in ('Б', 'КБ', 'МБ', 'ГБ', 'ТБ'):
        if n < 1024 or u == 'ТБ':
            return f'{n:.1f} {u}' if u != 'Б' else f'{n} Б'
        n /= 1024


def nrows(path):
    with open(path, 'rb') as f:
        return max(sum(buf.count(b'\n') for buf in iter(lambda: f.read(1 << 24), b'')) - 1, 0)


def ncols(path, sep=','):
    return len(pd.read_csv(path, nrows=0, sep=sep).columns)


def md(df, index=False):
    return df.to_markdown(index=index) if hasattr(df, 'to_markdown') else df.to_string(index=index)


def read_cols(path, cols, chunksize=500_000, **kw):
    have = pd.read_csv(path, nrows=0, **kw).columns
    use = [c for c in cols if c in have]
    return pd.concat(pd.read_csv(path, usecols=use, chunksize=chunksize, low_memory=False, **kw))


def exists(name):
    return os.path.exists(os.path.join(D, name))


P = lambda name: os.path.join(D, name)

p('# RUN_REPORT')
p()
# ---- a) what was run
p('## a) Запуск')
log = open(args.log).read() if os.path.exists(args.log) else ''
p(f'Версии: python {sys.version.split()[0]}, pandas {pd.__version__}; '
  + subprocess.run(['nextflow', '-v'], capture_output=True, text=True).stdout.strip().replace('\n', ' '))
p('Команда: `nohup bash run_full.sh > logs/run_YYYYMMDD.log &` (стадии a–d последовательно, каждая с `-resume`, до 5 перезапусков; затем `run_post.sh`).')
p()
p('Хронология (из RUN_LOG.md):')
p()
for line in log.splitlines():
    if re.match(r'^- \d{4}-\d\d-\d\d', line):
        p(line)
p()

# ---- b) files
p('## b) Файлы')
rows = []
for f in sorted(glob.glob(P('*.csv')) + glob.glob(P('*.tsv')) + glob.glob(P('*.txt'))):
    sep = '\t' if f.endswith('.tsv') else ','
    try:
        c = ncols(f, sep) if not f.endswith('.txt') else 1
        rows.append((os.path.relpath(f), fmt_size(os.path.getsize(f)), f'{nrows(f):,}', c))
    except Exception as e:
        rows.append((os.path.relpath(f), fmt_size(os.path.getsize(f)), '?', f'ошибка: {e}'))
p(md(pd.DataFrame(rows, columns=['путь', 'размер', 'строк', 'колонок'])) if rows else 'файлов в data/ нет')
for sub in ('sra_complete_runs', 'geo_cache', 'geo_state'):
    d = P(sub)
    if os.path.isdir(d):
        p(f'\n`{os.path.relpath(d)}/`: {len(os.listdir(d)):,} файлов')
p()

# ---- c) checks
p('## c) Проверки')
sra = None
if exists('sra_complete_runs.csv'):
    sra = read_cols(P('sra_complete_runs.csv'),
                    ['Run', 'ReleaseDate', 'LibraryStrategy', 'ScientificName', 'Model', 'bases'])
    p(f'- sra_complete_runs.csv: ранов {len(sra):,}; дубликатов Run: **{int(sra.Run.duplicated().sum())}**; '
      f'без LibraryStrategy: {sra.LibraryStrategy.isna().mean() * 100:.2f}%')
if exists('pre_filter_matrix.csv'):
    m = pd.read_csv(P('pre_filter_matrix.csv'))
    p(f'- pre_filter_matrix.csv: строк {len(m):,}; уникальных pmc_id {m.pmc_id.nunique():,}; уникальных accession {m.accession.nunique():,}')
if exists('pre_filter_dates.csv') and exists('pre_filter_matrix.csv'):
    d = pd.read_csv(P('pre_filter_dates.csv'))
    ids = d['File Name'].str.replace('.xml', '', regex=False)
    p(f'- статей без даты принятия: {(1 - ids.nunique() / m.pmc_id.nunique()) * 100:.1f}% '
      f'({m.pmc_id.nunique() - ids.nunique():,} из {m.pmc_id.nunique():,})')
mm = None
for name in ('metadata_matrix_filtered.csv', 'metadata_matrix_raw.csv'):
    if exists(name):
        mm = read_cols(P(name), ['pmc_ID', 'accession', 'converted_accession', 'library_strategy', 'repository',
                                 'reuse_role', 'pmc_date', 'repository_date', 'total_use_count', 'reuse_count'])
        mm_name = name
        break
if mm is not None:
    p(f'- {mm_name}: строк {len(mm):,}; уникальных pmc_ID {mm.pmc_ID.nunique():,}, accession {mm.accession.nunique():,}, '
      f'converted_accession {mm.converted_accession.nunique():,}')
    pairs = mm[['pmc_ID', 'converted_accession']].drop_duplicates()
    tuc = pairs.groupby('converted_accession').size()
    p(f'- доля датасетов с total_use_count>=2: {(tuc >= 2).mean() * 100:.1f}%, >=10: {(tuc >= 10).mean() * 100:.2f}% '
      f'(total_use_count = число уникальных статей на датасет; считается по парам pmc_ID×converted_accession)')
    if 'pmc_date' in mm:
        p(f'- строк без pmc_date: {mm.pmc_date.isna().mean() * 100:.1f}%')
# archives
al = sorted(glob.glob('work/*/*/archives.txt'), key=os.path.getmtime)
if al:
    names = [x.strip() for x in open(al[-1]) if x.strip()]
    done = {os.path.basename(f).replace('.matrix.csv', '') for f in glob.glob('work/*/*/*.matrix.csv')}
    miss = [n for n in names if n[:-7] not in done]
    p(f'- архивов Europe PMC в списке: {len(names):,}; обработано (есть matrix в work/): {len(names) - len(miss):,}; '
      f'необработано: {len(miss):,}' + (f' ({", ".join(miss[:20])}{" …" if len(miss) > 20 else ""})' if miss else ''))
else:
    p('- список архивов (archives.txt) в work/ не найден')
p()

# ---- d) SRA aggregates
p('## d) Агрегаты по sra_complete_runs.csv')
if sra is None:
    p('sra_complete_runs.csv отсутствует: этап SRA не завершён.')
else:
    vc = sra.LibraryStrategy.value_counts(dropna=False)
    p('**LibraryStrategy (раны, доля):**\n')
    p(md(pd.DataFrame({'ранов': vc, 'доля %': (vc / len(sra) * 100).round(2)}).reset_index().rename(columns={'index': 'LibraryStrategy'}), False))
    top15 = vc.index[:15]
    sra['year'] = pd.to_datetime(sra.ReleaseDate, errors='coerce').dt.year.astype('Int64')
    pv = pd.crosstab(sra.LibraryStrategy, sra.year).loc[[s for s in top15 if s in set(sra.LibraryStrategy)]]
    p('\n**Раны по LibraryStrategy × год ReleaseDate (топ-15):**\n')
    p(md(pv.reset_index()))
    p('\n**Топ-10 ScientificName:**\n')
    p(md(sra.ScientificName.value_counts().head(10).rename('ранов').reset_index()))
    g = (sra.bases / 1e9).groupby(sra.LibraryStrategy)
    q = pd.DataFrame({'медиана ГБ': g.median(), 'P90 ГБ': g.quantile(0.9)}).loc[[s for s in top15 if s in g.groups]].round(3)
    p('\n**bases (ГБ): медиана и P90, топ-15 стратегий:**\n')
    p(md(q.reset_index()))
    p('\n**Топ-10 Model:**\n')
    p(md(sra.Model.value_counts().head(10).rename('ранов').reset_index()))
p()

# ---- e) reuse aggregates
p('## e) Реальное переиспользование по LibraryStrategy')
if mm is None or 'library_strategy' not in mm:
    p('metadata_matrix_raw/filtered.csv отсутствует или без library_strategy: create/analyze не выполнены.')
else:
    pairs = mm[['pmc_ID', 'converted_accession', 'library_strategy']].drop_duplicates()
    tuc = pairs.groupby('converted_accession').size().rename('total_use_count')
    ds = pairs.drop_duplicates('converted_accession').set_index('converted_accession').join(tuc)
    agg = ds.groupby(ds.library_strategy.fillna('(нет LibraryStrategy)')).agg(
        датасетов=('total_use_count', 'size'), сумма_use=('total_use_count', 'sum'),
        медиана_use=('total_use_count', 'median'),
        доля_ge2=('total_use_count', lambda s: round((s >= 2).mean() * 100, 1)),
        доля_ge10=('total_use_count', lambda s: round((s >= 10).mean() * 100, 2)))
    papers = pairs.groupby(pairs.library_strategy.fillna('(нет LibraryStrategy)')).pmc_ID.nunique().rename('статей')
    p(md(agg.join(papers).sort_values('датасетов', ascending=False).reset_index()))
p()

# ---- f) global counts
p('## f) Глобальные счётчики SRA (esearch, db=sra; считаются эксперименты SRX, не раны)')
if exists('sra_strategy_counts.csv'):
    c = pd.read_csv(P('sra_strategy_counts.csv'))
    tot = c[c.year == 'all'].set_index('strategy')['count'].astype(int).sort_values(ascending=False)
    p('Топ-20 по общему числу:\n')
    p(md(tot.head(20).rename('count').reset_index()))
    yr = c[c.year != 'all'].copy()
    yr['year'] = yr.year.astype(int)
    pv = yr[yr.year >= 2015].pivot_table(index='strategy', columns='year', values='count', aggfunc='sum').loc[tot.head(20).index]
    p('\nПо годам 2015–2026 (топ-20):\n')
    p(md(pv.reset_index()))
else:
    p('data/sra_strategy_counts.csv отсутствует.')
p()

# ---- g/h
p('## g) Ошибки, повторы, правки кода')
logs = sorted(glob.glob('logs/run_*.log'))
for lf in logs:
    t = open(lf, errors='ignore').read()
    p(f'- {lf}: «terminated with an error» {t.count("terminated with an error")}, «Execution is retried» {t.count("Execution is retried")}')
p('\nПолный RUN_LOG.md (правки кода, решения):\n')
p(log)
p()
p('## h) Нерешённое')
p('- См. RUN_LOG.md: ключ NCBI_API_KEY не был задан (3 запроса/с), поэтому шаг SRA очень долгий.')
p('- Счётчики в разделе f — эксперименты (SRX), а не раны.')
p('- Ноутбуки: выполнение и ошибки отражены в logs/notebooks_*.log.')
open(args.out, 'w').write('\n'.join(out) + '\n')
print(f'wrote {args.out} ({len(out)} lines)')
