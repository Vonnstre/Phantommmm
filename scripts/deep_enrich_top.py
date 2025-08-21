#!/usr/bin/env python3
"""
Deep-enrich the top addresses listed in top500_for_deep.txt using Ethplorer freekey.
"""
import asyncio
import aiohttp
import argparse
import csv
import os
import sys
import time

ETHPLORER_ENDPOINT = "https://api.ethplorer.io/getAddressInfo/{}?apiKey=freekey"
COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"

async def fetch_json(session, url, retries=4, backoff=0.8):
    for i in range(retries):
        try:
            async with session.get(url, timeout=30) as r:
                if r.status == 200:
                    return await r.json()
                if r.status == 429:
                    ra = r.headers.get("Retry-After")
                    wait = float(ra) if ra and ra.isdigit() else backoff * (2 ** i)
                    await asyncio.sleep(wait)
                    continue
                await asyncio.sleep(backoff * (2 ** i))
        except Exception:
            await asyncio.sleep(backoff * (2 ** i))
    return None

async def enrich_address(session, addr, eth_usd):
    url = ETHPLORER_ENDPOINT.format(addr)
    data = await fetch_json(session, url)
    eth_balance = 0.0
    if data and 'ETH' in data and data['ETH'] and 'balance' in data['ETH']:
        try:
            eth_balance = float(data['ETH']['balance'])
        except Exception:
            eth_balance = 0.0
    eth_usd_value = eth_balance * (eth_usd or 0)

    tokens = (data or {}).get('tokens') or []
    total_token_balance_normalized = 0.0
    top_tokens = []
    for t in tokens:
        try:
            ti = t.get('tokenInfo') or {}
            decimals = int(ti.get('decimals') or 0) if ti.get('decimals') is not None else 0
            raw_bal = float(t.get('rawBalance') or t.get('balance') or 0)
            normalized = raw_bal / (10 ** decimals) if decimals > 0 else raw_bal
            total_token_balance_normalized += normalized
            top_tokens.append((ti.get('symbol') or ti.get('name') or ti.get('address'), normalized))
        except Exception:
            pass
    top_tokens = sorted(top_tokens, key=lambda x: -abs(x[1]))[:8]
    top_tokens_str = ";".join([f"{t[0]}:{t[1]:.6g}" for t in top_tokens])

    return {
        "address": addr,
        "eth_balance_deep": round(eth_balance, 12),
        "eth_usd_value_deep": round(eth_usd_value, 8),
        "total_token_balance_normalized_deep": round(total_token_balance_normalized, 6),
        "top_tokens_deep": top_tokens_str
    }

async def run_all(eth_usd, concurrency, top_file, out_path):
    if not os.path.exists(top_file):
        print("Missing top file:", top_file)
        return
    addrs = [l.strip() for l in open(top_file) if l.strip()]
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    connector = aiohttp.TCPConnector(limit=max(4, concurrency*2), force_close=True)
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        sem = asyncio.Semaphore(concurrency)
        results = []

        async def worker(a):
            async with sem:
                r = await enrich_address(session, a, eth_usd)
                if r:
                    results.append(r)

        tasks = [worker(a) for a in addrs]
        print(f"DEEP: launching {len(tasks)} tasks with concurrency={concurrency}")
        await asyncio.gather(*tasks)

    with open(out_path, 'w', newline='') as f:
        fieldnames = ["address","eth_balance_deep","eth_usd_value_deep",
                      "total_token_balance_normalized_deep","top_tokens_deep"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            w.writerow(r)

    print("WROTE deep file:", out_path, "rows:", len(results))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--eth_usd", type=float, default=None)
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--top_file", default="outputs/top500_for_deep.txt")
    p.add_argument("--out", default="outputs/whale_candidates_deep.csv")
    args = p.parse_args()

    import requests
    eth_usd = args.eth_usd
    if eth_usd is None:
        try:
            eth_usd = requests.get(COINGECKO_SIMPLE, timeout=10).json()["ethereum"]["usd"]
        except Exception:
            print("Could not fetch ETH price; set --eth_usd manually (optional). Proceeding with eth_usd=None.")
            eth_usd = None

    asyncio.run(run_all(eth_usd=eth_usd, concurrency=args.concurrency,
                        top_file=args.top_file, out_path=args.out))

if __name__ == "__main__":
    main()
