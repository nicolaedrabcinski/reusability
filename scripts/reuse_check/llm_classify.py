"""Label each (paper, accession) excerpt with a local LLM (vLLM offline, JSON-schema constrained).

Runs inside the vllm/vllm-openai image; uses only the standard library and vllm.
Input: contexts.jsonl (fetch_context.py). Output: labels.jsonl with the fields of SCHEMA plus pmc_ID,
accession, model. Pairs without any excerpt get role "unclear" without calling the model.
"""
import argparse
import json

from vllm import LLM, SamplingParams
from vllm.sampling_params import GuidedDecodingParams

CODEBOOK = """You read excerpts from a biomedical research article and decide what the article did with ONE dataset accession.

role:
- generated: the authors of THIS article produced the data behind this accession (ran the experiment or sequencing) and deposited it. Typical wording: "data generated in this study have been deposited in GEO under ...", "raw data are available under accession ...", a data availability statement about the study's own data.
- reused: the authors analysed data that already existed and that they obtained from a repository or resource, whoever produced it. Typical wording: "downloaded from GEO", "obtained from", "publicly available dataset", "previously published (ref)", "we re-analysed".
- mentioned_only: the accession is cited but its data are not analysed in this article (background, a list of related datasets, comparison with published results).
- unclear: the excerpt does not allow a decision.

same_group_earlier: true only if the text says the data were produced by the same authors in an EARLIER publication ("we previously reported", "our previous study", "as described in our earlier work").

data_taken (what was actually used; "na" for generated or mentioned_only):
- processed_geo: processed tables from GEO (series matrix, normalized or count tables, supplementary files, GEO2R).
- raw_reads: raw sequencing reads (FASTQ, SRA/ENA runs) re-aligned or re-processed by the authors.
- derived_resource: a secondary resource that re-hosts or re-processes the data (recount, ARCHS4, GEPIA, refine.bio, TCGA portals, other databases).
- not_stated.

id_source_mismatch: true if the cited identifier points to one repository but the text shows the data were actually taken from another one (e.g. a GSE is cited but raw reads were downloaded from SRA/ENA and re-processed; an SRA/PRJNA accession is cited but processed GEO tables were used). false otherwise.

evidence_quote: copy VERBATIM from the excerpt the single shortest sentence that justifies the role (max 300 characters). Empty string if there is none.
confidence: high, medium or low."""

SCHEMA = {
    'type': 'object',
    'properties': {
        'role': {'enum': ['generated', 'reused', 'mentioned_only', 'unclear']},
        'same_group_earlier': {'type': 'boolean'},
        'data_taken': {'enum': ['processed_geo', 'raw_reads', 'derived_resource', 'not_stated', 'na']},
        'id_source_mismatch': {'type': 'boolean'},
        'evidence_quote': {'type': 'string', 'maxLength': 400},
        'confidence': {'enum': ['high', 'medium', 'low']},
    },
    'required': ['role', 'same_group_earlier', 'data_taken', 'id_source_mismatch', 'evidence_quote', 'confidence'],
    'additionalProperties': False,
}

ap = argparse.ArgumentParser()
ap.add_argument('--contexts', required=True)
ap.add_argument('--model', required=True)
ap.add_argument('--max-model-len', type=int, default=8192)
ap.add_argument('--out', default='labels.jsonl')
args = ap.parse_args()

items = [json.loads(line) for line in open(args.contexts)]
todo = [it for it in items if it['context'].strip()]

llm = LLM(model=args.model, max_model_len=args.max_model_len, gpu_memory_utilization=0.90, seed=0)
tok = llm.get_tokenizer()


def prompt(it):
    user = (f"{CODEBOOK}\n\nAccession: {it['accession']}\n\nExcerpts (section path in brackets):\n"
            f"{it['context']}\n\nAnswer with the JSON object only.")
    return tok.apply_chat_template([{'role': 'user', 'content': user}], tokenize=False,
                                   add_generation_prompt=True, enable_thinking=False)


sp = SamplingParams(temperature=0.0, max_tokens=400, guided_decoding=GuidedDecodingParams(json=SCHEMA))
outs = llm.generate([prompt(it) for it in todo], sp)

labels = {}
for it, o in zip(todo, outs):
    try:
        lab = json.loads(o.outputs[0].text)
    except json.JSONDecodeError:
        lab = {'role': 'unclear', 'parse_error': o.outputs[0].text[:200]}
    labels[(it['pmc_ID'], it['accession'])] = lab

with open(args.out, 'w') as fo:
    for it in items:
        lab = labels.get((it['pmc_ID'], it['accession']), {'role': 'unclear', 'no_text': True})
        fo.write(json.dumps({'pmc_ID': it['pmc_ID'], 'accession': it['accession'], 'model': args.model,
                             **lab}, ensure_ascii=False) + '\n')
print(f'labelled {len(todo)} pairs with the model, {len(items) - len(todo)} without text')
