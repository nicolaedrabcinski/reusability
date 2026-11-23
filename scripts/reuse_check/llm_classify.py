"""Label each (paper, accession) pair with a local LLM (vLLM offline).

Input: contexts.jsonl from fetch_context.py (one excerpt per paper). One prompt per pair:
system prompt + the paper's excerpt + the accession; a paper's prompts share their prefix and go
one after another, so vLLM's prefix cache reuses the excerpt.

The model answers free text with a JSON object, validated against SCHEMA here. Constrained (guided)
decoding is NOT used: with vLLM 0.9.0 + Qwen3 it produced invented quotes, while the same prompt
without constraints quoted the excerpts verbatim (checked 2026-11-22). An answer that fails
validation is asked once more with a little sampling; if it fails again the pair is "unclear".

Output: labels.jsonl, one line per pair: pmc_ID, accession, model, the SCHEMA fields, quote_found
(the quote occurs in the excerpt, whitespace and case ignored), and the pass-through fields
text_source, n_mentions, raw_terms, processed_terms.
Runs inside the vllm/vllm-openai image; uses only the standard library and vllm.
"""
import argparse
import json
import re

from vllm import LLM, SamplingParams

SYSTEM = """You annotate biomedical research articles. You get excerpts from ONE article and ONE dataset accession, and you decide what the article did with that dataset. Base every answer only on the excerpts.

Fields, filled in this order:

evidence_quote: copy, character for character, the single sentence FROM THE EXCERPTS that best shows what was done with this accession (max 300 characters). Never copy text from these instructions. Empty string if no sentence in the excerpts is about this dataset.

role:
- generated: the authors of THIS article produced the data behind this accession (ran the experiment or the sequencing) and deposited it. A data availability statement that says where the data of this study can be found, accessed or downloaded (for example "data can be downloaded from GEO under ...", "are available under accession ...") means generated, unless it says the data were produced by others or come from an earlier publication, or the dataset carries a citation (a reference number or author and year), which means it was published before: then reused.
- reused: the authors analysed data that already existed and that they obtained from a repository or a resource, no matter who produced it (also when they say they downloaded, obtained, retrieved or re-analysed it, or cite a previous publication for it, their own included).
- mentioned_only: the accession is cited but its data are not analysed in this article (background, a list of related datasets, comparison with published results).
- unclear: the excerpts do not allow a decision.

same_group_earlier: true only if the excerpts say the data come from an EARLIER publication of the same authors. Otherwise false.

confidence: high, medium or low."""

SCHEMA = {
    'evidence_quote': str,
    'role': ('generated', 'reused', 'mentioned_only', 'unclear'),
    'same_group_earlier': bool,
    'confidence': ('high', 'medium', 'low'),
}
EXAMPLE = '{"evidence_quote": "...", "role": "...", "same_group_earlier": false, "confidence": "..."}'

ap = argparse.ArgumentParser()
ap.add_argument('--contexts', required=True)
ap.add_argument('--model', required=True)
ap.add_argument('--revision', default=None, help='model commit on Hugging Face (pinned for reproducibility)')
ap.add_argument('--max-model-len', type=int, default=8192)
ap.add_argument('--out', default='labels.jsonl')
ap.add_argument('--debug', type=int, default=0, help='print the first N prompts and answers, then exit')
args = ap.parse_args()

papers = [json.loads(line) for line in open(args.contexts)]
items = []   # one per pair, in paper order (prefix cache)
for p in papers:
    for q in p['pairs']:
        items.append({**q, 'pmc_ID': p['pmc_ID'], 'text_source': p['text_source'], 'context': p['context']})
todo = [it for it in items if it['context'].strip()]

llm = LLM(model=args.model, revision=args.revision, max_model_len=args.max_model_len,
          gpu_memory_utilization=0.90, enable_prefix_caching=True, seed=0)
tok = llm.get_tokenizer()


def prompt(it):
    user = (f"EXCERPTS (section path in brackets):\n<<<\n{it['context']}\n>>>\n\n"
            f"Accession: {it['accession']}\nAnswer with the JSON object only, like {EXAMPLE}")
    return tok.apply_chat_template([{'role': 'system', 'content': SYSTEM}, {'role': 'user', 'content': user}],
                                   tokenize=False, add_generation_prompt=True, enable_thinking=False)


def validate(text):
    m = re.search(r'\{.*\}', text, re.S)
    if not m:
        return None, 'no JSON object'
    try:
        lab = json.loads(m.group(0))
    except json.JSONDecodeError as e:
        return None, f'invalid JSON: {e}'
    for k, spec in SCHEMA.items():
        if k not in lab:
            return None, f'missing {k}'
        v = lab[k]
        if isinstance(spec, tuple) and v not in spec:
            return None, f'{k}={v!r} not allowed'
        if spec is bool and not isinstance(v, bool):
            return None, f'{k} not boolean'
        if spec is str and not isinstance(v, str):
            return None, f'{k} not a string'
    return {k: lab[k] for k in SCHEMA}, None


def norm(t):
    return re.sub(r'\s+', ' ', t).strip().lower()


if args.debug:
    for it, o in zip(todo[:args.debug], llm.generate([prompt(it) for it in todo[:args.debug]],
                                                     SamplingParams(temperature=0.0, max_tokens=400))):
        print('=' * 80, '\n', prompt(it)[-1200:], '\nANSWER:\n', o.outputs[0].text)
    raise SystemExit(0)

outs = llm.generate([prompt(it) for it in todo], SamplingParams(temperature=0.0, max_tokens=400))
answers = [validate(o.outputs[0].text) + (o.outputs[0].text,) for o in outs]
bad = [i for i, (lab, err, _) in enumerate(answers) if err]
if bad:   # one more try with a little sampling
    again = llm.generate([prompt(todo[i]) for i in bad],
                         SamplingParams(temperature=0.3, max_tokens=500, seed=1))
    for i, o in zip(bad, again):
        answers[i] = validate(o.outputs[0].text) + (o.outputs[0].text,)

labels, n_bad = {}, 0
for it, (lab, err, raw) in zip(todo, answers):
    if err:
        n_bad += 1
        lab = {'role': 'unclear', 'parse_error': err, 'raw_answer': raw[:500]}
    else:
        parts = [x for x in re.split(r'\.\.\.|…', lab['evidence_quote']) if x.strip()]   # ellipses allowed
        ctx = norm(it['context'])
        lab['quote_found'] = bool(parts) and all(norm(x) in ctx for x in parts)
    labels[(it['pmc_ID'], it['accession'])] = lab

with open(args.out, 'w') as fo:
    for it in items:
        lab = labels.get((it['pmc_ID'], it['accession']), {'role': 'unclear', 'no_text': True})
        fo.write(json.dumps({'pmc_ID': it['pmc_ID'], 'accession': it['accession'], 'model': args.model,
                             **lab, 'text_source': it['text_source'], 'n_mentions': it['n_mentions'],
                             'raw_terms': it['raw_terms'], 'processed_terms': it['processed_terms']},
                            ensure_ascii=False) + '\n')
print(f'pairs: {len(items)}, labelled by the model: {len(todo)} '
      f'(retried {len(bad)}, still invalid {n_bad}), without text: {len(items) - len(todo)}')
