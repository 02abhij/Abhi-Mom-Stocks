"""
QA Numerical Fluency Scorer
Reads Q->A pair JSON files, scores each pair via Claude API,
computes Spearman rho vs known price returns.

Usage: python score_qa_pairs.py
Requires: ANTHROPIC_API_KEY environment variable
"""

import os, json, time
import anthropic
from scipy import stats
import numpy as np

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

SYSTEM = """You are scoring earnings call Q&A pairs for management numerical fluency.

Score each pair 1 if management answers with a SPECIFIC operational number directly 
(e.g. "INR187 crores", "80% capacity", "17.9% EBITDA margin", "around 4% rate").
Score 0 if:
- Deflection: "we don't have", "difficult to say", "we'll come back", "too early"
- Vague answer without any specific number
- Passing to someone else without answering
- Very short yes/no that merely confirms analyst's own stated number

Return ONLY valid JSON, no other text: {"scores": [0,1,...], "scored_1": N, "total": M}"""

def score_transcript(label, pairs):
    qa = "\n\n".join(
        f"Pair {i+1}:\nQ: {p['q']}\nA: {p['a']}"
        for i, p in enumerate(pairs)
    )
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=400,
        system=SYSTEM,
        messages=[{"role": "user", "content": f"Score these {len(pairs)} pairs from {label}:\n\n{qa}"}]
    )
    text = msg.content[0].text.strip().replace("```json","").replace("```","").strip()
    return json.loads(text)

def spearman(xs, ys):
    rho, p = stats.spearmanr(xs, ys)
    return rho, p

def main():
    # Load all Q->A pair files
    qa_files = [f for f in os.listdir('.') if f.endswith('_qa.json')]
    if not qa_files:
        print("No *_qa.json files found. Run the extraction script first.")
        return

    results = []
    print(f"\n{'Label':<22} {'Pairs':>6} {'Fluent':>7} {'Rate':>7}   {'Zone':<8} Return")
    print("="*62)

    for fname in sorted(qa_files):
        with open(fname) as f:
            data = json.load(f)
        label   = data['label']
        pairs   = data['pairs']
        ret     = data['ret']
        outcome = data['outcome']

        result = score_transcript(label, pairs)
        rate   = result['scored_1'] / result['total'] if result['total'] > 0 else 0
        zone   = 'WINNER' if rate >= 0.55 else ('INTER' if rate >= 0.35 else 'LAGGARD')

        retstr = f"{ret:+}%" if ret is not None else "?"
        print(f"{label:<22} {result['total']:>6} {result['scored_1']:>7} {rate*100:>6.1f}%   {zone:<8} {retstr}")

        if ret is not None:
            results.append({'label': label, 'rate': rate, 'ret': ret, 'outcome': outcome})

        # save individual result
        with open(fname.replace('_qa.json', '_scored.json'), 'w') as f:
            json.dump({**data, 'fluency_rate': rate, 'scores': result['scores']}, f, indent=2)

        time.sleep(1)

    print(f"\n{'='*62}")
    if len(results) >= 4:
        rates   = np.array([r['rate'] for r in results])
        returns = np.array([r['ret']  for r in results])
        rho, p  = spearman(rates, returns)
        print(f"n={len(results)}  Spearman rho(fluency_rate vs return): {rho:+.3f}  p={p:.4f}")
        print()
        print("Ranked by fluency rate:")
        for r in sorted(results, key=lambda x: -x['rate']):
            bar = '█' * int(r['rate'] * 20)
            print(f"  {r['label']:<22} {bar:<12} {r['rate']*100:>5.1f}%  ret={r['ret']:+}%  [{r['outcome']}]")

if __name__ == "__main__":
    main()
