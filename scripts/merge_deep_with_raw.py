#!/usr/bin/env python3
"""
merge_deep_with_raw.py
Join deep CSV back into raw CSV producing merged outputs.
"""
import csv
import os
import argparse
import sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="outputs/whale_candidates_raw.csv")
    ap.add_argument("--deep", default="outputs/whale_candidates_deep.csv")
    ap.add_argument("--out", default="outputs/whale_candidates_merged.csv")
    args = ap.parse_args()

    RAW = args.raw
    DEEP = args.deep
    OUT = args.out

    if not os.path.exists(RAW) or not os.path.exists(DEEP):
        print("Missing raw or deep file.")
        raise SystemExit(1)

    deep_map = {}
    with open(DEEP, newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            deep_map[row['address'].lower()] = row

    with open(RAW, newline='') as fr, open(OUT, 'w', newline='') as fw:
        rr = csv.DictReader(fr)
        out_fields = rr.fieldnames + [
            "eth_balance_deep","eth_usd_value_deep",
            "total_token_balance_normalized_deep","top_tokens_deep"
        ]
        w = csv.DictWriter(fw, fieldnames=out_fields)
        w.writeheader()
        for row in rr:
            key = row['address'].lower()
            d = deep_map.get(key, {})
            out = dict(row)
            out["eth_balance_deep"] = d.get("eth_balance_deep","")
            out["eth_usd_value_deep"] = d.get("eth_usd_value_deep","")
            out["total_token_balance_normalized_deep"] = d.get("total_token_balance_normalized_deep","")
            out["top_tokens_deep"] = d.get("top_tokens_deep","")
            w.writerow(out)

    print("WROTE merged file:", OUT)

if __name__ == "__main__":
    main()
