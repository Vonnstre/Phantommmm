#!/usr/bin/env python3
"""
fetch_candidates.py
Fast pass: scrape Etherscan accounts pages for addresses, batch Cloudflare RPC balances,
write outputs/whale_candidates_raw.csv. No per-address token enrich here (that is the deep pass).
"""
import asyncio
import aiohttp
import argparse
import math
import time
import csv
import os
import sys
from datetime import datetime, timedelta
from itertools import islice

ETHERSCAN_ACCOUNTS_PAGE = "https://etherscan.io/accounts/{}"
CLOUDFLARE_RPC = "https://cloudflare-eth.com"
COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"

HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

# changeable batch size for JSON-RPC batching (bigger -> fewer HTTP requests)
DEFAULT_BATCH_SIZE = 50

def chunked(iterable, n):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, n))
        if not chunk:
            return
        yield chunk

async def fetch_text(session, url, headers=None, timeout=20, retries=2):
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=timeout, headers=headers) as r:
                if r.status == 200:
                    return await r.text()
                if r.status == 429:
                    ra = r.headers.get("Retry-After")
                    wait = float(ra) if ra and ra.isdigit() else (1 + attempt)
                    await asyncio.sleep(wait)
                    continue
                # other non-200 -> break
                return None
        except Exception:
            await asyncio.sleep(0.5 * (attempt + 1))
    return None

async def scrape_top_addresses(session, limit, concurrency=3):
    addresses = []
    page = 1
    sem = asyncio.Semaphore(concurrency)
    while len(addresses) < limit:
        url = ETHERSCAN_ACCOUNTS_PAGE.format(page)
        async with sem:
            text = await fetch_text(session, url, headers=HEADERS, timeout=20, retries=2)
        if not text:
            print(f"SCRAPE: page {page} returned no content / blocked — stopping.")
            break

        idx = 0
        found_this_page = 0
        while True:
            idx = text.find('/address/', idx)
            if idx == -1:
                break
            start = idx + len('/address/')
            addr = text[start:start + 42]
            if addr.startswith('0x') and len(addr) >= 42:
                addr = addr.split('"')[0].split('<')[0].strip().lower()
                addresses.append(addr)
                found_this_page += 1
                if len(addresses) >= limit:
                    break
            idx += 1

        print(f"SCRAPE: page {page} done, found {found_this_page} addresses (total so far: {len(addresses)})")
        page += 1
        await asyncio.sleep(0.45)

    # dedupe preserve order
    seen = set()
    out = []
    for a in addresses:
        if a not in seen:
            seen.add(a)
            out.append(a)
            if len(out) >= limit:
                break
    print(f"SCRAPE: finished - returning {len(out)} addresses")
    return out

async def batch_get_balances(session, addresses, batch_size=DEFAULT_BATCH_SIZE, timeout=30, retries=3):
    """
    Send batch JSON-RPC eth_getBalance calls to Cloudflare RPC.
    Returns dict address -> float(ETH) or None if failed.
    """
    results = {}
    headers = {"Content-Type": "application/json"}
    id_counter = 1
    for chunk in chunked(addresses, batch_size):
        # build batch request
        batch = []
        id_map = {}
        for a in chunk:
            req = {"jsonrpc": "2.0", "id": id_counter, "method": "eth_getBalance", "params": [a, "latest"]}
            id_map[id_counter] = a
            id_counter += 1
            batch.append(req)

        for attempt in range(retries):
            try:
                async with session.post(CLOUDFLARE_RPC, json=batch, headers=headers, timeout=timeout) as r:
                    if r.status == 200:
                        j = await r.json()
                        # j is a list of responses
                        for item in j:
                            addr = id_map.get(item.get("id"))
                            if not addr:
                                continue
                            res = item.get("result")
                            if res:
                                try:
                                    results[addr] = int(res, 16) / (10 ** 18)
                                except Exception:
                                    results[addr] = None
                            else:
                                results[addr] = None
                        break
                    elif r.status == 429:
                        ra = r.headers.get("Retry-After")
                        wait = float(ra) if ra and ra.isdigit() else (1 + attempt)
                        await asyncio.sleep(wait)
                        continue
                    else:
                        # on other non-200, try again a couple times
                        await asyncio.sleep(0.5 * (attempt + 1))
                # if reached here and processed chunk, move on
            except asyncio.TimeoutError:
                await asyncio.sleep(0.5 * (attempt + 1))
            except Exception:
                await asyncio.sleep(0.5 * (attempt + 1))
        # ensure addresses in chunk have an entry
        for a in chunk:
            results.setdefault(a, None)
        # polite pause (small)
        await asyncio.sleep(0.05)
    return results

def compute_eth_threshold(usd_threshold, eth_usd, eth_threshold_override):
    # if user provided eth_threshold (explicit), use that
    if eth_threshold_override is not None:
        return eth_threshold_override
    if eth_usd is None:
        return None
    return usd_threshold / eth_usd

async def run_all(limit, tx_lookback_days, usd_threshold, eth_usd, eth_threshold_override,
                  concurrency, batch_size, out_file, addresses_file):
    start = time.time()
    os.makedirs(os.path.dirname(out_file) or ".", exist_ok=True)
    timeout = aiohttp.ClientTimeout(total=60)
    conn = aiohttp.TCPConnector(limit=max(8, concurrency * 2), force_close=True)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        if addresses_file:
            addrs = []
            with open(addresses_file, 'r') as fh:
                for line in fh:
                    a = line.strip().lower()
                    if a.startswith('0x'):
                        addrs.append(a)
            print(f"LOADED: {len(addrs)} addresses from {addresses_file}")
        else:
            print(f"SCRAPE: starting (limit={limit})")
            addrs = await scrape_top_addresses(session, limit, concurrency=max(2, concurrency // 3))
            print(f"SCRAPE: scraped {len(addrs)} candidate addresses")

        if not addrs:
            print("No candidate addresses scraped; exiting.")
            return

        # compute threshold in ETH
        eth_threshold = compute_eth_threshold(usd_threshold, eth_usd, eth_threshold_override)
        if eth_threshold is None:
            print("ERROR: cannot determine ETH threshold because ETH price not provided.")
            print("Either provide --eth_usd or --eth_threshold (min ETH).")
            return
        print(f"Filtering threshold: USD {usd_threshold} -> ETH {eth_threshold:.6g}")

        # fetch balances in batches
        print(f"BALANCES: fetching balances for {len(addrs)} addresses using batch_size={batch_size}")
        balances = await batch_get_balances(session, addrs, batch_size=batch_size)
        # build results of addresses that meet eth_threshold
        candidates = []
        for a in addrs:
            bal = balances.get(a)  # may be None
            eth_usd_value = bal * eth_usd if (bal is not None and eth_usd is not None) else None
            if bal is not None and bal >= eth_threshold:
                candidates.append({
                    "address": a,
                    "eth_balance": round(bal, 12),
                    "eth_usd_value": round(eth_usd_value, 8) if eth_usd is not None else None,
                    "last_tx_ts": None,
                    "days_since_last_tx": None,
                    "tx_count_30d": 0,
                    "tx_count_90d": 0,
                    "tx_count_365d": 0,
                    "token_actions_30d": 0,
                    "token_actions_90d": 0,
                    "distinct_tokens_ever": 0,
                    "recv_from_exchange_count_lookback": 0,
                    "send_to_exchange_count_lookback": 0,
                    "is_contract": 0,
                    "total_in_tokens_normalized": 0.0,
                    "total_out_tokens_normalized": 0.0,
                    "top_tokens": "",
                    "lead_score_candidate": 0.0,  # finalize will recompute fuller score
                })
        print(f"FILTER: {len(candidates)} addresses meet ETH threshold (>= {eth_threshold:.6g} ETH)")

        if not candidates:
            print("No results above threshold.")
            return

        # write raw CSV (fields match finalize_sqlite expectation)
        fieldnames = ["address","eth_balance","eth_usd_value","last_tx_ts","days_since_last_tx",
                      "tx_count_30d","tx_count_90d","tx_count_365d","token_actions_30d","token_actions_90d",
                      "distinct_tokens_ever","recv_from_exchange_count_lookback","send_to_exchange_count_lookback",
                      "is_contract","total_in_tokens_normalized","total_out_tokens_normalized",
                      "top_tokens","lead_score_candidate"]
        with open(out_file, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for row in sorted(candidates, key=lambda x: (x["eth_usd_value"] or 0.0), reverse=True):
                w.writerow(row)

        elapsed = time.time() - start
        print(f"WROTE {out_file} rows={len(candidates)} time={elapsed:.1f}s")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--tx_lookback_days", type=int, default=90)
    p.add_argument("--usd_threshold", type=float, default=10000)
    p.add_argument("--eth_usd", type=float, default=None, help="override ETH/USD (float)")
    p.add_argument("--eth_threshold", type=float, default=None, help="override threshold in ETH (float)")
    p.add_argument("--concurrency", type=int, default=6)
    p.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE)
    p.add_argument("--out", default="outputs/whale_candidates_raw.csv")
    p.add_argument("--addresses_file", default=None)
    args = p.parse_args()

    import requests
    eth_usd = args.eth_usd
    if eth_usd is None:
        # try fetch coinGecko with retries
        ok = False
        for attempt in range(3):
            try:
                resp = requests.get(COINGECKO_SIMPLE, timeout=10)
                if resp.status_code == 200:
                    eth_usd = resp.json().get("ethereum", {}).get("usd")
                    if eth_usd:
                        ok = True
                        break
            except Exception:
                pass
            time.sleep(1 + attempt)
        if not ok and args.eth_threshold is None:
            print("ERROR: could not fetch ETH price from CoinGecko and --eth_threshold not provided.")
            print("Re-run with --eth_usd <price> or --eth_threshold <min_eth>.")
            sys.exit(1)

    print("START: fetch_candidates.py", {
        "limit": args.limit,
        "usd_threshold": args.usd_threshold,
        "concurrency": args.concurrency,
        "batch_size": args.batch_size,
        "eth_usd": eth_usd,
        "eth_threshold_override": args.eth_threshold
    })

    asyncio.run(run_all(limit=args.limit, tx_lookback_days=args.tx_lookback_days,
                        usd_threshold=args.usd_threshold, eth_usd=eth_usd,
                        eth_threshold_override=args.eth_threshold,
                        concurrency=args.concurrency, batch_size=args.batch_size,
                        out_file=args.out, addresses_file=args.addresses_file))

if __name__ == "__main__":
    main()
