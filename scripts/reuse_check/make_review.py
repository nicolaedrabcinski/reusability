"""Merge model labels with the pairs, score the model against the GEO-link reference, add the
rule-based data-source signals, and list only the pairs a person needs to look at.

Reference without manual work (GEO's own paper link):
  geo_G_linked -> generated;  geo_R_notlinked -> not generated (reused or mentioned_only)

Rule-based fields (from the paragraphs that mention the accession, see fetch_context.py):
  data_taken_rule      raw_reads | processed_geo | both | not_stated   (only for role = reused)
  id_source_mismatch   geo_cited_raw_used  (a GEO series cited, raw reads re-processed)
                       sra_cited_geo_used  (an SRA/BioProject accession cited, processed GEO tables used)

Outputs:
  labels_all.csv.gz  every pair: pair fields, model fields, rule fields, review reason
  review_todo.csv    pairs for a person, at most --max-todo, spread over the review reasons
  review_summary.md  agreement with the reference, roles by stratum, flags
"""
import argparse
import json

import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument('--pairs', required=True)
ap.add_argument('--labels', required=True, nargs='+', help='one or more labels.jsonl files')
ap.add_argument('--audit', type=int, default=10, help='random extra pairs for a sanity check')
ap.add_argument('--max-todo', type=int, default=100)
ap.add_argument('--seed', type=int, default=20261122)
args = ap.parse_args()

pairs = pd.read_csv(args.pairs, dtype=str)
lab = pd.DataFrame([json.loads(x) for f in args.labels for x in open(f)])
df = pairs.merge(lab, on=['pmc_ID', 'accession'], how='left')
for c in ('evidence_quote', 'parse_error', 'raw_answer', 'confidence', 'same_group_earlier', 'model',
          'quote_found', 'text_source', 'n_mentions', 'raw_terms', 'processed_terms', 'role'):
    if c not in df:
        df[c] = pd.NA
df['role'] = df['role'].fillna('missing')


def listlen(v):
    return len(v) if isinstance(v, list) else 0


raw = df['raw_terms'].map(listlen) > 0
proc = df['processed_terms'].map(listlen) > 0
reused = df['role'].eq('reused')
df['data_taken_rule'] = 'na'
df.loc[reused, 'data_taken_rule'] = 'not_stated'
df.loc[reused & raw & ~proc, 'data_taken_rule'] = 'raw_reads'
df.loc[reused & proc & ~raw, 'data_taken_rule'] = 'processed_geo'
df.loc[reused & raw & proc, 'data_taken_rule'] = 'both'
df['id_source_mismatch'] = ''
df.loc[reused & df.repository.eq('GEO') & df.data_taken_rule.eq('raw_reads'), 'id_source_mismatch'] = 'geo_cited_raw_used'
df.loc[reused & df.repository.eq('SRA') & df.data_taken_rule.eq('processed_geo'), 'id_source_mismatch'] = 'sra_cited_geo_used'

ref = {'geo_G_linked': 'generated', 'geo_R_notlinked': 'not_generated'}
df['reference'] = df['stratum'].map(ref)
df['agrees_with_reference'] = [
    None if pd.isna(want) or role in ('unclear', 'missing') else (role == 'generated') == (want == 'generated')
    for want, role in zip(df['reference'], df['role'])]
heur_gen = df['reuse_role'].eq('G')
model_decided = df['role'].isin(['generated', 'reused', 'mentioned_only'])
df['model_vs_heuristic'] = model_decided & (df['role'].eq('generated') != heur_gen)


def reasons(r):
    out = []
    if r.text_source == 'none':
        return 'no_full_text'
    if isinstance(r.parse_error, str):
        out.append('model_answer_invalid')
    if r.agrees_with_reference is not None and not r.agrees_with_reference:
        out.append('model_vs_geo_link')
    if r.role == 'unclear' or r.confidence == 'low':
        out.append('model_unsure')
    if r.role in ('generated', 'reused', 'mentioned_only') and str(r.quote_found) != 'True':
        out.append('quote_not_in_text')
    if r.id_source_mismatch:
        out.append('id_source_mismatch')
    if r.stratum in ('geo_G_notlinked', 'geo_R_linked') and r.model_vs_heuristic:
        out.append('model_vs_date_heuristic')
    return ';'.join(out)


df['review_reason'] = df.apply(reasons, axis=1)
rest = df[df['review_reason'].eq('')]
df.loc[rest.sample(min(args.audit, len(rest)), random_state=args.seed).index, 'review_reason'] = 'random_audit'
df['url'] = 'https://pmc.ncbi.nlm.nih.gov/articles/' + df['pmc_ID'] + '/'
df.drop(columns=['raw_answer']).to_csv('labels_all.csv.gz', index=False)

flagged = df[df['review_reason'].ne('') & df['review_reason'].ne('no_full_text')].copy()
flagged['first_reason'] = flagged['review_reason'].str.split(';').str[0]
if len(flagged) > args.max_todo:   # spread the person's work over the reasons
    per = max(1, args.max_todo // flagged['first_reason'].nunique())
    flagged = (flagged.groupby('first_reason', group_keys=False)
               .apply(lambda g: g.sample(min(per, len(g)), random_state=args.seed)))
keep = ['stratum', 'review_reason', 'pmc_ID', 'accession', 'url', 'reuse_role', 'link_status', 'role',
        'confidence', 'data_taken_rule', 'id_source_mismatch', 'same_group_earlier', 'evidence_quote',
        'quote_found', 'parse_error']
todo = flagged.reindex(columns=keep)
todo['human_role'] = ''
todo['human_note'] = ''
todo.to_csv('review_todo.csv', index=False)

lines = ['# Text check of generator/reuser roles (model vs GEO link)', '',
         f'Model: {df["model"].dropna().iloc[0] if df["model"].notna().any() else "n/a"}; '
         f'pairs: {len(df):,}; papers: {df.pmc_ID.nunique():,}', '',
         '## Agreement with the GEO-link reference', '']
for st, want in ref.items():
    sub = df[(df.stratum == st) & df.agrees_with_reference.notna()]
    ok = int(sub.agrees_with_reference.astype(bool).sum())
    n_unclear = ((df.stratum == st) & df.role.isin(['unclear', 'missing'])).sum()
    lines.append(f'- {st} (reference: {want}): {ok:,} / {len(sub):,} agree'
                 + (f' ({100 * ok / len(sub):.1f}%)' if len(sub) else '') + f'; unclear: {n_unclear:,}')
lines += ['', '## Model role by stratum', '', '```', pd.crosstab(df.stratum, df.role, margins=True).to_string(), '```', '',
          '## Model vs date heuristic', '',
          '```', pd.crosstab(df.reuse_role, df.role, margins=True).to_string(), '```', '',
          '## Data source of reused pairs (rules on the paragraphs with the accession)', '',
          '```', pd.crosstab(df[reused].repository, df[reused].data_taken_rule, margins=True).to_string()
          if reused.any() else '(none)', '```', '',
          '## Other flags', '',
          f'- pairs without full text: {(df.text_source == "none").sum():,}',
          f'- accession not found in the text: {(pd.to_numeric(df.n_mentions, errors="coerce").fillna(0) == 0).sum():,}',
          f'- quote not found in the excerpt (model decided): {(model_decided & df.quote_found.astype(str).ne("True")).sum():,}',
          f'- model answers that failed validation twice: {df.parse_error.notna().sum():,}',
          f'- id_source_mismatch: {df.id_source_mismatch.value_counts().drop("", errors="ignore").to_dict()}', '',
          f'## To review by a person: {len(todo)} pairs (review_todo.csv)', '',
          '```', todo.review_reason.str.split(';').explode().value_counts().to_string() if len(todo) else '(none)', '```']
open('review_summary.md', 'w').write('\n'.join(lines) + '\n')
print('\n'.join(lines))
