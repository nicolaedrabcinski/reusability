"""Merge model labels with the pair table, score the model against the GEO-link reference, and list
only the pairs a person needs to look at.

Reference without manual work (from GEO's own paper link):
  geo_G_linked     -> generated
  geo_R_notlinked  -> not generated (reused or mentioned_only)
Other strata have no reference; the model's answer there is what we want to learn.

Outputs:
  review_all.csv     every pair with model fields, quote check and review reason
  review_todo.csv    only pairs needing a person (empty human_* columns to fill)
  review_summary.md  agreement with the reference, label distribution per stratum
"""
import argparse
import json
import re

import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument('--pairs', required=True)
ap.add_argument('--contexts', required=True)
ap.add_argument('--labels', required=True)
ap.add_argument('--audit', type=int, default=10, help='random extra pairs for a sanity check')
ap.add_argument('--seed', type=int, default=20261122)
args = ap.parse_args()

pairs = pd.read_csv(args.pairs, dtype=str)
ctx = pd.DataFrame([json.loads(x) for x in open(args.contexts)])
lab = pd.DataFrame([json.loads(x) for x in open(args.labels)])
df = pairs.merge(ctx, on=['pmc_ID', 'accession'], how='left').merge(lab, on=['pmc_ID', 'accession'], how='left')
for c in ('evidence_quote', 'rationale', 'parse_error', 'raw_answer', 'confidence', 'data_taken',
          'id_source_mismatch', 'same_group_earlier', 'model'):
    if c not in df:
        df[c] = pd.NA


def norm(t):
    return re.sub(r'\s+', ' ', str(t)).strip().lower()


df['quote_found'] = [bool(q) and norm(q) in norm(c) for q, c in zip(df['evidence_quote'].fillna(''), df['context'].fillna(''))]
ref = {'geo_G_linked': 'generated', 'geo_R_notlinked': 'not_generated'}
df['reference'] = df['stratum'].map(ref)
df['agrees_with_reference'] = [
    None if pd.isna(want) or role in ('unclear', None) or pd.isna(role)
    else (role == 'generated') == (want == 'generated')
    for want, role in zip(df['reference'], df['role'])]


def is_true(v):
    return str(v) == 'True'   # works for Python bool, numpy bool and NaN


def reasons(r):
    out = []
    if r.text_source == 'none':
        out.append('no_full_text')
    if isinstance(r.get('parse_error'), str):
        out.append('model_answer_invalid')
    if r.agrees_with_reference is not None and not r.agrees_with_reference:
        out.append('model_vs_geo_link')
    if r.text_source != 'none' and (r.role == 'unclear' or r.confidence == 'low'):
        out.append('model_unsure')
    if r.text_source != 'none' and r.role != 'unclear' and not r.quote_found:
        out.append('quote_not_in_text')
    if is_true(r.id_source_mismatch):
        out.append('id_source_mismatch')
    if r.stratum in ('geo_G_notlinked', 'geo_R_linked') and r.role in ('generated', 'reused'):
        heuristic_gen = r.reuse_role == 'G'
        if (r.role == 'generated') != heuristic_gen:
            out.append('model_vs_date_heuristic')
    return ';'.join(out)


df['review_reason'] = df.apply(reasons, axis=1)
rest = df[df['review_reason'].eq('') & df['text_source'].ne('none')]
audit = rest.sample(min(args.audit, len(rest)), random_state=args.seed).index
df.loc[audit, 'review_reason'] = 'random_audit'
df['url'] = 'https://pmc.ncbi.nlm.nih.gov/articles/' + df['pmc_ID'] + '/'
for c in ('human_role', 'human_note'):
    df[c] = ''

keep = ['stratum', 'review_reason', 'pmc_ID', 'accession', 'url', 'reuse_role', 'link_status', 'role',
        'confidence', 'data_taken', 'id_source_mismatch', 'same_group_earlier', 'evidence_quote', 'rationale',
        'quote_found', 'parse_error', 'human_role', 'human_note']
df.drop(columns=['context']).to_csv('review_all.csv', index=False)
todo = df[df['review_reason'].ne('') & df['review_reason'].ne('no_full_text')][keep]
todo.to_csv('review_todo.csv', index=False)

lines = ['# Text check of generator/reuser roles (model vs GEO link)', '',
         f'Model: {df["model"].dropna().iloc[0] if df["model"].notna().any() else "n/a"}', '',
         '## Agreement with the GEO-link reference', '']
for st, want in ref.items():
    sub = df[(df.stratum == st) & df.agrees_with_reference.notna()]
    n_unclear = ((df.stratum == st) & df.role.eq('unclear')).sum()
    ok = int(sub.agrees_with_reference.astype(bool).sum())
    lines.append(f'- {st} (reference: {want}): {ok} / {len(sub)} agree'
                 + (f' ({100 * ok / len(sub):.0f}%)' if len(sub) else '') + f'; unclear: {n_unclear}')
lines += ['', '## Model role by stratum', '', '```', pd.crosstab(df.stratum, df.role.fillna('missing')).to_string(), '```', '',
          '## Other flags', '',
          f'- pairs without full text: {(df.text_source == "none").sum()} of {len(df)}',
          f'- accession not found in the text: {(df.n_mentions.fillna(0).astype(int) == 0).sum()}',
          f'- evidence quote not found verbatim (model answered): {((~df.quote_found) & df.role.ne("unclear") & df.text_source.ne("none")).sum()}',
          f'- model answers that failed validation: {df["parse_error"].notna().sum()}',
          f'- id_source_mismatch flagged: {df.id_source_mismatch.map(is_true).sum()}',
          f'- data_taken: {df.data_taken.value_counts().to_dict()}', '',
          f'## To review by a person: {len(todo)} pairs (review_todo.csv)', '',
          '```', todo.review_reason.str.split(';').explode().value_counts().to_string(), '```']
open('review_summary.md', 'w').write('\n'.join(lines) + '\n')
print('\n'.join(lines))
