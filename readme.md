# Whale Candidates Pipeline (No-API-Keys)

Fetch high-value Ethereum addresses using free endpoints:
- **Candidates (fast)**: Scrape Etherscan accounts pages + Cloudflare RPC balances.
- **Finalize**: Score, redact sample, create `top500_for_deep.txt`, zip outputs.
- **Deep (optional)**: Ethplorer free key (freekey) for top 500 details.
- **Merge**: Join deep columns back to raw and finalize again.

## Outputs (in `outputs/`)
- `whale_candidates_raw.csv` — candidates (fast pass).
- `final_whales.csv` — scored/ordered final.
- `redacted_top10.csv` — sample for outreach.
- `top500_for_deep.txt` — addresses for deep pass.
- `whale_candidates_deep.csv` — deep results (if deep step ran).
- `whale_candidates_merged.csv` — merged raw+deep (if deep step ran).
- `final_package.zip` — bundle to deliver.

## Local usage
```bash
python -m pip install -r requirements.txt

# 1) FAST pass (produces whale_candidates_raw.csv)
python scripts/fetch_candidates.py \
  --limit 3000 --fast --usd_threshold 10000 --concurrency 8 \
  --out outputs/whale_candidates_raw.csv

# 2) Finalize (first pass)
python scripts/finalize_sqlite.py --raw outputs/whale_candidates_raw.csv --base outputs

# 3) Deep-enrich top 500 (optional)
python scripts/deep_enrich_top.py --top_file outputs/top500_for_deep.txt \
  --out outputs/whale_candidates_deep.csv --concurrency 4

# 4) Merge deep back and finalize again
python scripts/merge_deep_with_raw.py \
  --raw outputs/whale_candidates_raw.csv \
  --deep outputs/whale_candidates_deep.csv \
  --out outputs/whale_candidates_merged.csv

python scripts/finalize_sqlite.py --raw outputs/whale_candidates_merged.csv --base outputs
