"""Latent class model over the independent signals: how often each signal is right, and for every
pair the probability that the paper generated the dataset, with no manual labels.

Raters (binary; empty = not observed): s_heuristic, s_repo_link, s_authors (pair_signals.py) and
s_llm from the text check (generated -> 1, reused / mentioned_only -> 0, unclear -> empty), read from
every *.labels.jsonl under --labels-dir that exists at run time.

Model (Dawid-Skene / Hui-Walter with missing ratings): a latent truth z (1 = generated); prevalence
P(z=1) per group (repository x data class); sensitivity and specificity per rater and repository,
the same in all groups of that repository; ratings independent given z. Fitted by EM on the counts of
distinct rating patterns. 95% intervals: --boot parametric-free bootstrap replicates (resampling
pairs). The fit assumes conditional independence; the dependence table compares observed and fitted
joint frequencies for each pair of raters, and large ratios mean the assumption is broken there.

Outputs: lca_pairs.csv.gz (signals, p_generated, lca_role, confident), lca_raters.csv, lca_summary.md
"""
import argparse
import glob
import json

import numpy as np
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument('--signals', required=True)
ap.add_argument('--labels-dir', required=True)
ap.add_argument('--boot', type=int, default=200)
ap.add_argument('--seed', type=int, default=20261122)
args = ap.parse_args()

df = pd.read_csv(args.signals, dtype={'pmc_ID': str, 'accession': str})
files = sorted(glob.glob(f'{args.labels_dir}/*.labels.jsonl'))
llm = {}
for f in files:
    for line in open(f):
        d = json.loads(line)
        llm[(d['pmc_ID'], d['accession'])] = {'generated': 1, 'reused': 0, 'mentioned_only': 0}.get(d.get('role'))
df['s_llm'] = pd.array([llm.get(k) for k in zip(df.pmc_ID, df.accession)], dtype='Int64')
RATERS = ['s_heuristic', 's_repo_link', 's_authors', 's_llm']
REPOS = sorted(df.repository.dropna().unique())
df['group'] = df.repository.fillna('NA') + '|' + df['class'].fillna('NA')
GROUPS = sorted(df.group.unique())

Y = np.stack([df[c].fillna(-1).astype(int).to_numpy() for c in RATERS], axis=1)
g_idx = df.group.map({g: i for i, g in enumerate(GROUPS)}).to_numpy()
r_idx = df.repository.map({r: i for i, r in enumerate(REPOS)}).to_numpy()
keys = np.column_stack([g_idx, r_idx, Y])
pat, inv, cnt = np.unique(keys, axis=0, return_inverse=True, return_counts=True)
inv = inv.ravel()
pg, pr, py = pat[:, 0], pat[:, 1], pat[:, 2:]
J, R, G = len(RATERS), len(REPOS), len(GROUPS)


def em(n, se=None, sp=None, pi=None, iters=3000, tol=1e-10):
    se = np.full((J, R), 0.8) if se is None else se.copy()
    sp = np.full((J, R), 0.8) if sp is None else sp.copy()
    pi = np.full(G, 0.3) if pi is None else pi.copy()
    last = -np.inf
    for _ in range(iters):
        l1 = np.log(pi[pg])
        l0 = np.log(1 - pi[pg])
        for j in range(J):
            y = py[:, j]
            s1, s0 = se[j, pr], sp[j, pr]
            l1 = l1 + np.where(y == 1, np.log(s1), np.where(y == 0, np.log(1 - s1), 0))
            l0 = l0 + np.where(y == 1, np.log(1 - s0), np.where(y == 0, np.log(s0), 0))
        m = np.maximum(l1, l0)
        ll = float(np.sum(n * (m + np.log(np.exp(l1 - m) + np.exp(l0 - m)))))
        w = 1 / (1 + np.exp(l0 - l1))
        pi = np.clip(np.bincount(pg, n * w, G) / np.maximum(np.bincount(pg, n, G), 1e-12), 1e-4, 1 - 1e-4)
        for j in range(J):
            y = py[:, j]
            o = y >= 0
            num1 = np.bincount(pr[o], (n * w * (y == 1))[o], R)
            den1 = np.bincount(pr[o], (n * w)[o], R)
            num0 = np.bincount(pr[o], (n * (1 - w) * (y == 0))[o], R)
            den0 = np.bincount(pr[o], (n * (1 - w))[o], R)
            se[j] = np.where(den1 > 0, np.clip(num1 / np.maximum(den1, 1e-12), 1e-4, 1 - 1e-4), np.nan)
            sp[j] = np.where(den0 > 0, np.clip(num0 / np.maximum(den0, 1e-12), 1e-4, 1 - 1e-4), np.nan)
        se, sp = np.nan_to_num(se, nan=0.5), np.nan_to_num(sp, nan=0.5)
        if abs(ll - last) < tol * max(1, abs(ll)):
            break
        last = ll
    return se, sp, pi, w, ll


se, sp, pi, w, ll = em(cnt.astype(float))
# label switching guard: the date heuristic must be better than chance in every repository
for r in range(R):
    if se[0, r] + sp[0, r] < 1:
        raise SystemExit('fit converged to the swapped solution; check the signals')
df['p_generated'] = w[inv]
df['lca_role'] = np.where(df.p_generated >= 0.5, 'G', 'R')
df['confident'] = (df.p_generated >= 0.95) | (df.p_generated <= 0.05)

rng = np.random.default_rng(args.seed)
boot_se, boot_sp, boot_pi = [], [], []
N = int(cnt.sum())
for _ in range(args.boot):
    nb = rng.multinomial(N, cnt / N).astype(float)
    s1, s0, p1, _, _ = em(nb, se, sp, pi, iters=500, tol=1e-8)
    boot_se.append(s1)
    boot_sp.append(s0)
    boot_pi.append(p1)
boot_se, boot_sp, boot_pi = np.array(boot_se), np.array(boot_sp), np.array(boot_pi)

rows = []
for j, rt in enumerate(RATERS):
    for r, rp in enumerate(REPOS):
        n_obs = int(((df.repository == rp) & df[rt].notna()).sum())
        if n_obs == 0:
            continue
        rows.append({'rater': rt, 'repository': rp, 'n_observed': n_obs,
                     'sensitivity': se[j, r], 'se_lo': np.percentile(boot_se[:, j, r], 2.5), 'se_hi': np.percentile(boot_se[:, j, r], 97.5),
                     'specificity': sp[j, r], 'sp_lo': np.percentile(boot_sp[:, j, r], 2.5), 'sp_hi': np.percentile(boot_sp[:, j, r], 97.5)})
raters = pd.DataFrame(rows)
raters.to_csv('lca_raters.csv', index=False)

# conditional dependence check: observed vs fitted P(both say "generated") and P(both say "not")
dep = []
for r, rp in enumerate(REPOS):
    sub = df[df.repository == rp]
    gi = sub.group.map({g: i for i, g in enumerate(GROUPS)}).to_numpy()
    for a in range(J):
        for b in range(a + 1, J):
            m = sub[RATERS[a]].notna() & sub[RATERS[b]].notna()
            if m.sum() < 50:
                continue
            ya, yb, pg_ = sub.loc[m, RATERS[a]].astype(int), sub.loc[m, RATERS[b]].astype(int), pi[gi[m.to_numpy()]]
            e11 = np.mean(pg_ * se[a, r] * se[b, r] + (1 - pg_) * (1 - sp[a, r]) * (1 - sp[b, r]))
            e00 = np.mean(pg_ * (1 - se[a, r]) * (1 - se[b, r]) + (1 - pg_) * sp[a, r] * sp[b, r])
            o11, o00 = np.mean((ya == 1) & (yb == 1)), np.mean((ya == 0) & (yb == 0))
            dep.append({'repository': rp, 'pair': f'{RATERS[a]} x {RATERS[b]}', 'n': int(m.sum()),
                        'obs_both_1': o11, 'fit_both_1': e11, 'obs_both_0': o00, 'fit_both_0': e00,
                        'ratio_1': o11 / e11 if e11 else np.nan, 'ratio_0': o00 / e00 if e00 else np.nan})
dep = pd.DataFrame(dep)

out_cols = ['pmc_ID', 'accession', 'converted_accession', 'repository', 'class', 'stratum', 'reuse_role'] + RATERS + \
           ['s_refine', 'p_generated', 'lca_role', 'confident']
df[out_cols].to_csv('lca_pairs.csv.gz', index=False)


def fmt(x):
    return f'{x:.3f}'


lines = ['# Latent class check of generator/reuser roles (no manual labels)', '',
         f'Pairs: {len(df):,}; model labels read from {len(files)} chunk file(s): {df.s_llm.notna().sum():,} pairs with a decided model answer.',
         f'Log-likelihood {ll:,.1f}; distinct rating patterns {len(pat):,}; bootstrap replicates {args.boot}.', '',
         '## How often each signal is right (95% bootstrap interval)', '',
         '| rater | repository | pairs rated | sensitivity (says G when G) | specificity (says R when R) |', '|---|---|---|---|---|']
for _, r in raters.iterrows():
    lines.append(f'| {r.rater} | {r.repository} | {r.n_observed:,} | {fmt(r.sensitivity)} [{fmt(r.se_lo)}–{fmt(r.se_hi)}] '
                 f'| {fmt(r.specificity)} [{fmt(r.sp_lo)}–{fmt(r.sp_hi)}] |')
lines += ['', '## Share of pairs where the paper generated the data (fitted prevalence)', '', '| group | pairs | prevalence [95%] |', '|---|---|---|']
for i, g in enumerate(GROUPS):
    lines.append(f'| {g} | {(df.group == g).sum():,} | {fmt(pi[i])} [{fmt(np.percentile(boot_pi[:, i], 2.5))}–{fmt(np.percentile(boot_pi[:, i], 97.5))}] |')
heur_R = (df.reuse_role == 'R').sum()
exp_R = float((1 - df.p_generated).sum())
lines += ['', '## Date heuristic vs latent class', '', '```',
          pd.crosstab(df.reuse_role, df.lca_role, margins=True).rename_axis(index='heuristic', columns='lca').to_string(), '```', '',
          f'- Reuse pairs (R): heuristic {heur_R:,}; expected under the model {exp_R:,.0f} ({exp_R / heur_R - 1:+.1%}).',
          f'- Confident pairs (p >= 0.95 or <= 0.05): {df.confident.sum():,} of {len(df):,} ({df.confident.mean():.1%}); '
          f'uncertain: {(~df.confident).sum():,}.',
          f'- Pairs whose heuristic role the model would flip with p >= 0.95: '
          f'{(((df.reuse_role == "R") & (df.p_generated >= 0.95)) | ((df.reuse_role == "G") & (df.p_generated <= 0.05))).sum():,}.', '']
rf = df[df.s_refine.notna()]
if len(rf):
    lines += ['## Cross-check with refine.bio (not independent of s_repo_link)', '',
              f'- pairs covered: {len(rf):,}; latent class agrees with refine.bio: {((rf.lca_role == "G") == (rf.s_refine == 1)).mean():.1%}', '']
lines += ['## Conditional dependence check', '', 'Ratio = observed / fitted joint frequency; close to 1 means the independence assumption holds for that pair of raters.', '']
if len(dep):
    lines += ['| repository | raters | pairs | both G: obs / fit | both R: obs / fit |', '|---|---|---|---|---|']
    for _, d in dep.iterrows():
        lines.append(f'| {d.repository} | {d.pair} | {d.n:,} | {fmt(d.obs_both_1)} / {fmt(d.fit_both_1)} ({d.ratio_1:.2f}) '
                     f'| {fmt(d.obs_both_0)} / {fmt(d.fit_both_0)} ({d.ratio_0:.2f}) |')
lines += ['', '## Coverage of the signals', '', '```',
          df.groupby('repository')[RATERS + ['s_refine']].agg(lambda x: f'{x.notna().mean():.1%}').to_string(), '```']
open('lca_summary.md', 'w').write('\n'.join(lines) + '\n')
print('\n'.join(lines))
