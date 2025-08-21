#!/usr/bin/env python3
"""
fetch_candidates.py

Default behavior:
 - If --addresses_file provided: read addresses from that file.
 - Else: scrape Etherscan /accounts/<page> pages until `--limit` addresses found.

Fast mode (--fast): only eth_getBalance via Cloudflare RPC (cheap & fast) — use to scan many addresses.
Deep mode (default off with --fast): also call Ethplorer per-address (may be throttled).

Stream CSV rows as they pass --usd_threshold to reduce memory usage.
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
from typing import Optional

ETHERSCAN_ACCOUNTS_PAGE = "https://etherscan.io/accounts/{}"
ETHPLORER_ENDPOINT = "https://api.ethplorer.io/getAddressInfo/{}?apiKey=freekey"
CLOUDFLARE_RPC = "https://cloudflare-eth.com"
COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"

EXCHANGE_ADDRESSES = {
    "0x742d35cc6634c0532925a3b844bc454e4438f44e",
    "0xbe0eb53f46cd790cd13851d5efff43d12404d33e8",
    "0xdc76cd25977e0a5ae17155770273ad58648900d3",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; whale-fetcher/1.0)"}


async def fetch_json(session: aiohttp.ClientSession, url: str, method: str = 'GET',
                     json: Optional[dict] = None, headers: Optional[dict] = None,
                     timeout: int = 20, retries: int = 3, backoff: float = 0.6):
    for attempt in range(retries):
        try:
            if method.upper() == 'GET':
                async with session.get(url, timeout=timeout, headers=headers) as r:
                    if r.status == 200:
                        return await r.json()
                    if r.status == 429:
                        ra = r.headers.get('Retry-After')
                        wait = float(ra) if ra and ra.isdigit() else backoff * (2 ** attempt)
                        await asyncio.sleep(wait)
                        continue
                    # client error -> return None (not retrying further)
                    if 400 <= r.status < 500:
                        return None
            else:
                async with session.post(url, json=json, timeout=timeout, headers=headers) as r:
                    if r.status == 200:
                        return await r.json()
                    if r.status == 429:
                        ra = r.headers.get('Retry-After')
                        wait = float(ra) if ra and ra.isdigit() else backoff * (2 ** attempt)
                        await asyncio.sleep(wait)
                        continue
                    if 400 <= r.status < 500:
                        return None
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass
        await asyncio.sleep(backoff * (2 ** attempt))
    return None


async def scrape_top_addresses(session: aiohttp.ClientSession, limit: int, concurrency: int = 3):
    """
    Scrape Etherscan /accounts/<page> pages to collect addresses until `limit`.
    Returns deduped list preserving order. Stops early if Etherscan returns non-200.
    """
    addresses = []
    page = 1
    sem = asyncio.Semaphore(concurrency)
    while len(addresses) < limit:
        url = ETHERSCAN_ACCOUNTS_PAGE.format(page)
        async with sem:
            try:
                async with session.get(url, headers=HEADERS, timeout=20) as r:
                    if r.status != 200:
                        print(f"SCRAPE: page {page} returned status {r.status} — stopping scrape.")
                        break
                    text = await r.text()
            except Exception as e:
                print(f"SCRAPE: page {page} fetch error: {e} — stopping.")
                break

        idx = 0
        found_this = 0
        while True:
            idx = text.find('/address/', idx)
            if idx == -1:
                break
            start = idx + len('/address/')
            addr = text[start:start + 42]
            if addr.startswith('0x') and len(addr) >= 42:
                addr = addr.split('"')[0].split('<')[0].strip().lower()
                addresses.append(addr)
                found_this += 1
                if len(addresses) >= limit:
                    break
            idx += 1

        print(f"SCRAPE: page {page} found {found_this}; total so far {len(addresses)}")
        page += 1
        # polite delay so Etherscan doesn't block quickly
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
    print(f"SCRAPE: finished; returning {len(out)} addresses")
    return out


async def get_balance_rpc(session: aiohttp.ClientSession, address: str):
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_getBalance", "params": [address, "latest"]}
    r = await fetch_json(session, CLOUDFLARE_RPC, method='POST', json=payload,
                         headers={"Content-Type": "application/json"}, retries=4)
    if not r or 'result' not in r:
        return None
    try:
        return int(r['result'], 16) / (10 ** 18)
    except Exception:
        return None


async def fetch_ethplorer(session: aiohttp.ClientSession, address: str):
    url = ETHPLORER_ENDPOINT.format(address)
    return await fetch_json(session, url, retries=4, backoff=0.6)


def balance_tier_label(usd_val: float):
    try:
        v = float(usd_val or 0)
    except Exception:
        return "unknown"
    if v >= 10_000_000:
        return ">=10M"
    if v >= 1_000_000:
        return ">=1M"
    if v >= 100_000:
        return ">=100k"
    if v >= 10_000:
        return ">=10k"
    if v >= 1_000:
        return ">=1k"
    return "<1k"


async def process_address(session: aiohttp.ClientSession, address: str, eth_usd: float,
                          lookback_days: int, exchange_set: set, deep: bool = True):
    tasks = [get_balance_rpc(session, address)]
    if deep:
        tasks.append(fetch_ethplorer(session, address))
    else:
        tasks.append(asyncio.sleep(0, result=None))
    bal_rpc, ethplorer = await asyncio.gather(*tasks)

    now = datetime.utcnow()
    eth_balance = bal_rpc or 0.0
    eth_usd_value = eth_balance * eth_usd if eth_usd is not None else None

    # default fields
    distinct_tokens = 0
    total_in_tokens = 0.0
    total_out_tokens = 0.0
    top_tokens = []
    tx_count_30 = tx_count_90 = tx_count_365 = 0
    token_actions_30 = token_actions_90 = 0
    recv_from_exchange = send_to_exchange = 0
    last_ts = None
    seen_tokens_set = set()
    is_contract = False

    if ethplorer:
        tokens = ethplorer.get('tokens') or []
        distinct_tokens = len(tokens)
        for t in tokens:
            try:
                ti = t.get('tokenInfo') or {}
                decimals = int(ti.get('decimals') or 0) if ti.get('decimals') is not None else 0
                raw_bal = float(t.get('rawBalance') or t.get('balance') or 0)
                normalized = raw_bal / (10 ** decimals) if decimals > 0 else raw_bal
                top_tokens.append((ti.get('symbol') or ti.get('name') or ti.get('address'), normalized))
            except Exception:
                pass
        top_tokens = sorted(top_tokens, key=lambda x: -abs(x[1]))[:8]

        txs = ethplorer.get('transactions') or []
        for tx in txs:
            ts = tx.get('timestamp') or tx.get('time') or tx.get('date')
            try:
                ts = int(ts)
            except Exception:
                continue
            if last_ts is None or ts > last_ts:
                last_ts = ts
            if ts >= int((now - timedelta(days=30)).timestamp()):
                tx_count_30 += 1
            if ts >= int((now - timedelta(days=90)).timestamp()):
                tx_count_90 += 1
            if ts >= int((now - timedelta(days=365)).timestamp()):
                tx_count_365 += 1
            if 'tokenInfo' in tx or 'tokenSymbol' in tx or tx.get('isTokenTransfer'):
                if ts >= int((now - timedelta(days=30)).timestamp()):
                    token_actions_30 += 1
                if ts >= int((now - timedelta(days=90)).timestamp()):
                    token_actions_90 += 1
                val = 0.0
                try:
                    val = float(tx.get('value') or 0)
                except Exception:
                    val = 0.0
                frm = (tx.get('from') or '').lower()
                to = (tx.get('to') or '').lower()
                ti = tx.get('tokenInfo') or {}
                decimals = 0
                try:
                    decimals = int(ti.get('decimals') or 0)
                except Exception:
                    decimals = 0
                normalized = val / (10 ** decimals) if decimals and val else val
                if to == address:
                    total_in_tokens += normalized
                if frm == address:
                    total_out_tokens += normalized
                token_addr = (ti.get('address') or tx.get('token') or tx.get('tokenAddress') or '').lower()
                if token_addr:
                    seen_tokens_set.add(token_addr)
            frm = (tx.get('from') or '').lower()
            to = (tx.get('to') or '').lower()
            if frm in exchange_set and to == address:
                recv_from_exchange += 1
            if to in exchange_set and frm == address:
                send_to_exchange += 1
        is_contract = bool(ethplorer.get('contractInfo') or ethplorer.get('isContract') or False)

    if last_ts:
        last_tx_dt = datetime.utcfromtimestamp(last_ts).isoformat()
        days_since_last = (datetime.utcnow() - datetime.utcfromtimestamp(last_ts)).days
    else:
        last_tx_dt = None
        days_since_last = 99999

    distinct_tokens_ever = max(distinct_tokens, len(seen_tokens_set))
    concentration_score = 0 if distinct_tokens_ever == 0 else max(0.0, 1.0 - (distinct_tokens_ever / 20.0))
    size_score = math.log10(max((eth_balance * (eth_usd or 1.0)), 1.0)) * 40
    lead_score = (
        size_score
        + min(20, tx_count_90) * 1.5
        + min(10, token_actions_30) * 1.0
        + (15 if days_since_last <= 30 else 0)
        + (5 if recv_from_exchange > 0 else 0)
        + max(0.0, (total_in_tokens - total_out_tokens)) * 0.01
        + (concentration_score * 5)
    )

    return {
        "address": address,
        "eth_balance": round(eth_balance, 12),
        "eth_usd_value": round((eth_balance * (eth_usd or 0)), 8) if eth_usd is not None else None,
        "balance_tier": balance_tier_label(eth_balance * (eth_usd or 0)),
        "last_tx_ts": last_tx_dt,
        "days_since_last_tx": days_since_last,
        "tx_count_30d": tx_count_30,
        "tx_count_90d": tx_count_90,
        "tx_count_365d": tx_count_365,
        "token_actions_30d": token_actions_30,
        "token_actions_90d": token_actions_90,
        "distinct_tokens_ever": distinct_tokens_ever,
        "recv_from_exchange_count_lookback": recv_from_exchange,
        "send_to_exchange_count_lookback": send_to_exchange,
        "is_contract": int(bool(is_contract)),
        "total_in_tokens_normalized": round(total_in_tokens, 6),
        "total_out_tokens_normalized": round(total_out_tokens, 6),
        "top_tokens": ";".join([f"{t[0]}:{t[1]:.6g}" for t in top_tokens]),
        "lead_score_candidate": round(lead_score, 6),
    }


async def run_all(limit: int, lookback_days: int, usd_threshold: float, eth_usd: float,
                  concurrency: int, deep: bool, out_file: str, addresses_file: Optional[str]):

    start = time.time()
    os.makedirs(os.path.dirname(out_file) or ".", exist_ok=True)
    connector = aiohttp.TCPConnector(limit=max(8, concurrency * 2), force_close=True)
    timeout = aiohttp.ClientTimeout(total=90)

    # CSV fields (include deep columns placeholders)
    fieldnames = [
        "address", "eth_balance", "eth_usd_value", "balance_tier",
        "last_tx_ts", "days_since_last_tx",
        "tx_count_30d", "tx_count_90d", "tx_count_365d",
        "token_actions_30d", "token_actions_90d", "distinct_tokens_ever",
        "recv_from_exchange_count_lookback", "send_to_exchange_count_lookback",
        "is_contract", "total_in_tokens_normalized", "total_out_tokens_normalized",
        "top_tokens", "lead_score_candidate"
    ]

    write_lock = asyncio.Lock()
    total_written = 0

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # obtain address list
        if addresses_file:
            addrs = []
            with open(addresses_file, 'r') as fh:
                for line in fh:
                    a = line.strip().lower()
                    if a.startswith('0x'):
                        addrs.append(a)
            print(f"LOADED {len(addrs)} addresses from {addresses_file}")
        else:
            print(f"SCRAPE: will scrape up to {limit} addresses from Etherscan accounts pages")
            addrs = await scrape_top_addresses(session, limit, concurrency=max(2, concurrency // 3))
            print(f"SCRAPE: scraped {len(addrs)} addresses")

        if not addrs:
            print("ERROR: No addresses available (scrape failed or file empty). Exiting.")
            return

        sem = asyncio.Semaphore(concurrency)
        processed = 0
        total = len(addrs)

        # prepare output CSV (streaming)
        f_out = open(out_file, 'w', newline='')
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        async def worker(addr):
            async with sem:
                try:
                    return await process_address(session, addr, eth_usd, lookback_days, EXCHANGE_ADDRESSES, deep=deep)
                except Exception as e:
                    print(f"WORKER ERROR for {addr[:10]}...: {e}")
                    return None

        tasks = [worker(a) for a in addrs[:limit]]
        print(f"WORK: launching {len(tasks)} tasks with concurrency={concurrency}")

        try:
            for fut in asyncio.as_completed(tasks):
                r = await fut
                processed += 1
                if r is None:
                    # failed to fetch this address
                    if processed % 100 == 0:
                        print(f"PROGRESS: processed={processed}/{total}")
                    continue
                # check USD threshold (eth_usd_value can be None if eth_usd unavailable)
                val = r.get("eth_usd_value") or 0
                if val >= usd_threshold:
                    # stream-write the row
                    async with write_lock:
                        writer.writerow({k: r.get(k, "") for k in fieldnames})
                        total_written += 1
                if processed % 200 == 0 or processed == total:
                    elapsed = time.time() - start
                    print(f"PROGRESS: processed={processed}/{total} matches={total_written} elapsed={elapsed:.1f}s")
        finally:
            f_out.close()

        elapsed = time.time() - start
        print(f"WROTE {out_file} rows={total_written} processed={processed} time={elapsed:.1f}s")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=10000, help="Maximum addresses to scan (default 10k). For fast scans you can set 100k+ but watch GH time.")
    p.add_argument("--tx_lookback_days", type=int, default=90)
    p.add_argument("--usd_threshold", type=float, default=10000)
    p.add_argument("--eth_usd", type=float, default=None, help="Override ETH/USD (float) to avoid remote fetch")
    p.add_argument("--concurrency", type=int, default=12)
    p.add_argument("--out", default="outputs/whale_candidates_raw.csv")
    p.add_argument("--fast", action="store_true", help="If set, skip Ethplorer deep per-address calls (recommended for large scans)")
    p.add_argument("--addresses_file", default=None, help="Optional: path with one address per line (if provided, will not scrape)")
    args = p.parse_args()

    import requests
    eth_usd = args.eth_usd
    if eth_usd is None:
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
        if not ok:
            print("ERROR: could not fetch ETH price from CoinGecko and --eth_usd not provided. Use --eth_usd to set price and continue.")
            sys.exit(1)

    print("START:", {"limit": args.limit, "usd_threshold": args.usd_threshold, "concurrency": args.concurrency, "fast": args.fast, "eth_usd": eth_usd})

    asyncio.run(run_all(limit=args.limit, lookback_days=args.tx_lookback_days,
                        usd_threshold=args.usd_threshold, eth_usd=eth_usd,
                        concurrency=args.concurrency, deep=not args.fast,
                        out_file=args.out, addresses_file=args.addresses_file))


if __name__ == "__main__":
    main()
