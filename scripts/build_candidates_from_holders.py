#!/usr/bin/env python3
"""
build_candidates_from_holders.py

Scrape token-holder pages (Etherscan) for curated tokens and produce a deduplicated candidates file.

This script is intentionally conservative and polite: it stops if pages return non-200 and sleeps between requests.

Note: Etherscan rate-limits heavily for scraping. Use paid APIs for production.
"""

import asyncio
import aiohttp
import argparse
import time
import os
import csv

TOKENS = {
    # token symbol: token-address (mainnet)
    "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "DAI":  "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    "stETH": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
}

ETHERSCAN_HOLDERS = "https://etherscan.io/token/generic-tokenholders2?a={}&p={}&ps=100"
HEADERS = {"User-Agent": "Mozilla/5.0"}


async def fetch_page(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=20) as r:
            if r.status != 200:
                return None
            return await r.text()
    except Exception:
        return None


def extract_addresses_from_html(text):
    out = []
    idx = 0
    while True:
        idx = text.find('/address/', idx)
        if idx == -1:
            break
        start = idx + len('/address/')
        addr = text[start:start + 42]
        if addr.startswith('0x') and len(addr) >= 42:
            addr = addr.split('"')[0].split('<')[0].strip().lower()
            out.append(addr)
        idx += 1
    return out


async def scrape_token_holders(token_address, per_token, concurrency):
    connector = aiohttp.TCPConnector(limit=max(4, concurrency * 2), force_close=True)
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        holders = []
        page = 1
        while len(holders) < per_token:
            url = ETHERSCAN_HOLDERS.format(token_address, page)
            text = await fetch_page(session, url)
            if not text:
                print(f"HOLDERS: page {page} returned no content or non-200; stopping for token {token_address}")
                break
            found = extract_addresses_from_html(text)
            if not found:
                break
            for a in found:
                holders.append(a)
                if len(holders) >= per_token:
                    break
            page += 1
            await asyncio.sleep(0.35)
        # dedupe preserve order
        seen = set()
        out = []
        for a in holders:
            if a not in seen:
                seen.add(a)
                out.append(a)
                if len(out) >= per_token:
                    break
        return out


async def run_all(per_token, concurrency, out_file):
    os.makedirs(os.path.dirname(out_file) or '.', exist_ok=True)
    results = []
    for sym, addr in TOKENS.items():
        print(f"SCRAPING HOLDERS: token={sym} address={addr} per_token={per_token}")
        res = await scrape_token_holders(addr, per_token, concurrency)
        print(f"SCRAPED: token={sym} got={len(res)}")
        results.extend(res)
    # dedupe global
    seen = set()
    final = []
    for a in results:
        if a not in seen:
            seen.add(a)
            final.append(a)
    with open(out_file, 'w', newline='') as f:
        w = csv.writer(f)
        for a in final:
            w.writerow([a])
    print(f"WROTE candidates file {out_file} rows={len(final)}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--per_token', type=int, default=500, help='Max holders to pull per token')
    p.add_argument('--concurrency', type=int, default=4)
    p.add_argument('--out', default='outputs/candidates_from_holders.txt')
    args = p.parse_args()

    asyncio.run(run_all(per_token=args.per_token, concurrency=args.concurrency, out_file=args.out))


if __name__ == '__main__':
    main()
