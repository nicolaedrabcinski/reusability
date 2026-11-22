"""Label each (paper, accession) excerpt with a local LLM (vLLM offline).

The model answers free text with a JSON object; the answer is then validated against SCHEMA here.
Constrained (guided) decoding is NOT used: with vLLM 0.9.0 + Qwen3 it produced invented quotes,
while the same prompt without constraints quoted the excerpts verbatim (checked 2026-11-22).

Runs inside the vllm/vllm-openai image; uses only the standard library and vllm.
Input: contexts.jsonl (fetch_context.py). Output: labels.jsonl with the fields of SCHEMA plus pmc_ID,
accession, model. Pairs without any excerpt get role "unclear" without calling the model.
"""
import argparse
import json

from vllm import LLM, SamplingParams
import re

SYSTEM = """You annotate biomedical research articles. You get excerpts from ONE article and ONE dataset accession, and you decide what the article did with that dataset. Base every answer only on the excerpts.

Fields, filled in this order:

evidence_quote: copy, character for character, the single sentence FROM THE EXCERPTS that best shows what was done with this accession (max 300 characters). Never copy text from these instructions. Empty string if no sentence in the excerpts is about this dataset.

rationale: one or two short sentences explaining the decision.

role:
- generated: the authors of THIS article produced the data behind this accession (ran the experiment or the sequencing) and deposited it.
- reused: the authors analysed data that already existed and that they obtained from a repository or a resource, no matter who produced it (also when they say they downloaded, obtained, retrieved or re-analysed it, or cite a previous publication for it).
- mentioned_only: the accession is cited but its data are not analysed in this article (background, a list of related datasets, comparison with published results).
- unclear: the excerpts do not allow a decision.

same_group_earlier: true only if the excerpts say the data come from an EARLIER publication of the same authors. Otherwise false.

data_taken: what was actually used. Must be "na" when role is generated, mentioned_only or unclear. When role is reused:
- processed_geo: processed tables from GEO (series matrix, normalized or count tables, supplementary files, GEO2R);
- raw_reads: raw sequencing reads (FASTQ, SRA or ENA runs) that the authors aligned or processed themselves;
- derived_resource: a secondary resource that re-hosts or re-processes the data (recount, ARCHS4, GEPIA, refine.bio, TCGA portals, other databases);
- not_stated: the excerpts do not say.

id_source_mismatch: true if the cited identifier belongs to one repository but the excerpts show the data were taken from another (for example a GEO series is cited but raw reads were downloaded from SRA or ENA and processed; an SRA or BioProject accession is cited but processed GEO tables were used). Otherwise false.

confidence: high, medium or low."""

SCHEMA = {
    'type': 'object',
    'properties': {
        'evidence_quote': {'type': 'string', 'maxLength': 400},
        'rationale': {'type': 'string', 'maxLength': 400},
        'role': {'enum': ['generated', 'reused', 'mentioned_only', 'unclear']},
        'same_group_earlier': {'type': 'boolean'},
        'data_taken': {'enum': ['processed_geo', 'raw_reads', 'derived_resource', 'not_stated', 'na']},
        'id_source_mismatch': {'type': 'boolean'},
        'confidence': {'enum': ['high', 'medium', 'low']},
    },
    'required': ['evidence_quote', 'rationale', 'role', 'same_group_earlier', 'data_taken',
                 'id_source_mismatch', 'confidence'],
    'additionalProperties': False,
}

ap = argparse.ArgumentParser()
ap.add_argument('--contexts', required=True)
ap.add_argument('--model', required=True)
ap.add_argument('--revision', default=None, help='model commit on Hugging Face (pinned for reproducibility)')
ap.add_argument('--max-model-len', type=int, default=8192)
ap.add_argument('--out', default='labels.jsonl')
ap.add_argument('--debug', type=int, default=0,
                help='print the first N prompts and free (unconstrained) answers, then exit')
args = ap.parse_args()

items = [json.loads(line) for line in open(args.contexts)]
todo = [it for it in items if it['context'].strip()]

llm = LLM(model=args.model, revision=args.revision, max_model_len=args.max_model_len, gpu_memory_utilization=0.90, seed=0)
tok = llm.get_tokenizer()


def prompt(it):
    user = (f"EXCERPTS (section path in brackets):\n<<<\n{it['context']}\n>>>\n\n"
            f"Accession: {it['accession']}\nAnswer with the JSON object only.")
    return tok.apply_chat_template([{'role': 'system', 'content': SYSTEM}, {'role': 'user', 'content': user}],
                                   tokenize=False, add_generation_prompt=True, enable_thinking=False)


if args.debug:
    sub = todo[:args.debug]
    free = llm.generate([prompt(it) for it in sub], SamplingParams(temperature=0.0, max_tokens=600))
    for it, o in zip(sub, free):
        p = prompt(it)
        print('=' * 80, '\nPROMPT TOKENS:', len(tok(p)['input_ids']), '\nPROMPT TAIL:\n', p[-1500:],
              '\nFREE ANSWER:\n', o.outputs[0].text)
    raise SystemExit(0)

def validate(text):
    """Return (label dict, error or None). Takes the outermost {...} of the answer."""
    m = re.search(r'\{.*\}', text, re.S)
    if not m:
        return None, 'no JSON object'
    try:
        lab = json.loads(m.group(0))
    except json.JSONDecodeError as e:
        return None, f'invalid JSON: {e}'
    for k, spec in SCHEMA['properties'].items():
        if k not in lab:
            return None, f'missing {k}'
        v = lab[k]
        if 'enum' in spec and v not in spec['enum']:
            return None, f'{k}={v!r} not allowed'
        if spec.get('type') == 'boolean' and not isinstance(v, bool):
            return None, f'{k} not boolean'
        if spec.get('type') == 'string' and not isinstance(v, str):
            return None, f'{k} not a string'
    return {k: lab[k] for k in SCHEMA['properties']}, None


sp = SamplingParams(temperature=0.0, max_tokens=600)
outs = llm.generate([prompt(it) for it in todo], sp)

labels, n_bad = {}, 0
for it, o in zip(todo, outs):
    lab, err = validate(o.outputs[0].text)
    if err:
        n_bad += 1
        lab = {'role': 'unclear', 'parse_error': err, 'raw_answer': o.outputs[0].text[:500]}
    labels[(it['pmc_ID'], it['accession'])] = lab

with open(args.out, 'w') as fo:
    for it in items:
        lab = labels.get((it['pmc_ID'], it['accession']), {'role': 'unclear', 'no_text': True})
        fo.write(json.dumps({'pmc_ID': it['pmc_ID'], 'accession': it['accession'], 'model': args.model,
                             **lab}, ensure_ascii=False) + '\n')
print(f'labelled {len(todo)} pairs with the model ({n_bad} answers failed validation), {len(items) - len(todo)} without text')
