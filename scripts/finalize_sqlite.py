#!/usr/bin/env python3
"""
finalize_sqlite.py

Reads raw (or merged) CSV and produces final_whales.csv, redacted_top10.csv, top500_for_deep.txt, README.txt and final_package.zip.
"""

import sqlite3
import csv
import os
import zipfile
import datetime
import math
import argparse


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="outputs/whale_candidates_raw.csv", help="Input CSV (raw or merged)")
    ap.add_argument("--base", default="outputs", help="Output directory")
    args = ap.parse_args()

    BASE = args.base
    RAW = args.raw
    OUT = os.path.join(BASE, "final_whales.csv")
    SAMPLE = os.path.join(BASE, "redacted_top10.csv")
    README = os.path.join(BASE, "README.txt")
    ZIP = os.path.join(BASE, "final_package.zip")
    TOP_FOR_DEEP = os.path.join(BASE, "top500_for_deep.txt")
    RUN_ID = "run_" + datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    PRODUCED_AT = datetime.datetime.utcnow().isoformat() + "Z"

    os.makedirs(BASE, exist_ok=True)
    if not os.path.exists(RAW):
        print("ERROR: raw CSV not found:", RAW)
        raise SystemExit(1)

    con = sqlite3.connect(":memory:")
    cur = con.cursor()

    cur.execute("PRAGMA journal_mode=OFF;")
    con.create_function("LOG10", 1, lambda x: math.log10(float(x)) if x is not None and float(x) > 0 else 0.0)

    cur.execute("""
        CREATE TABLE raw(
            address TEXT, eth_balance TEXT, eth_usd_value TEXT, balance_tier TEXT, last_tx_ts TEXT,
            days_since_last_tx TEXT, tx_count_30d TEXT, tx_count_90d TEXT, tx_count_365d TEXT,
            token_actions_30d TEXT, token_actions_90d TEXT, distinct_tokens_ever TEXT,
            recv_from_exchange_count_lookback TEXT, send_to_exchange_count_lookback TEXT,
            is_contract TEXT, total_in_tokens_normalized TEXT, total_out_tokens_normalized TEXT,
            top_tokens TEXT, lead_score_candidate TEXT,
            eth_balance_deep TEXT, eth_usd_value_deep TEXT,
            total_token_balance_normalized_deep TEXT, top_tokens_deep TEXT, protocols_hint TEXT
        );
    """)

    with open(RAW, newline='') as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            rows.append((
                r.get("address",""),
                r.get("eth_balance",""),
                r.get("eth_usd_value",""),
                r.get("balance_tier",""),
                r.get("last_tx_ts",""),
                r.get("days_since_last_tx",""),
                r.get("tx_count_30d",""),
                r.get("tx_count_90d",""),
                r.get("tx_count_365d",""),
                r.get("token_actions_30d",""),
                r.get("token_actions_90d",""),
                r.get("distinct_tokens_ever",""),
                r.get("recv_from_exchange_count_lookback",""),
                r.get("send_to_exchange_count_lookback",""),
                r.get("is_contract",""),
                r.get("total_in_tokens_normalized",""),
                r.get("total_out_tokens_normalized",""),
                r.get("top_tokens",""),
                r.get("lead_score_candidate",""),
                r.get("eth_balance_deep",""),
                r.get("eth_usd_value_deep",""),
                r.get("total_token_balance_normalized_deep",""),
                r.get("top_tokens_deep",""),
                r.get("protocols_hint",""),
            ))
        cur.executemany("INSERT INTO raw VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        con.commit()

    cur.execute(f"""
        CREATE VIEW final AS
        SELECT
          address,
          CAST(eth_balance AS REAL) AS eth_balance,
          CAST(eth_usd_value AS REAL) AS eth_usd_value,
          balance_tier,
          last_tx_ts,
          COALESCE(CAST(days_since_last_tx AS INTEGER), 99999) AS days_since_last_tx,
          COALESCE(CAST(tx_count_30d AS INTEGER),0) AS tx_count_30d,
          COALESCE(CAST(tx_count_90d AS INTEGER),0) AS tx_count_90d,
          COALESCE(CAST(tx_count_365d AS INTEGER),0) AS tx_count_365d,
          COALESCE(CAST(token_actions_30d AS INTEGER),0) AS token_actions_30d,
          COALESCE(CAST(token_actions_90d AS INTEGER),0) AS token_actions_90d,
          COALESCE(CAST(distinct_tokens_ever AS INTEGER),0) AS distinct_tokens_ever,
          COALESCE(CAST(recv_from_exchange_count_lookback AS INTEGER),0) AS recv_from_exchange_count_lookback,
          COALESCE(CAST(send_to_exchange_count_lookback AS INTEGER),0) AS send_to_exchange_count_lookback,
          CASE WHEN COALESCE(CAST(is_contract AS INTEGER),0) != 0 THEN 1 ELSE 0 END AS is_contract,
          COALESCE(CAST(total_in_tokens_normalized AS REAL),0.0) AS total_in_tokens_normalized,
          COALESCE(CAST(total_out_tokens_normalized AS REAL),0.0) AS total_out_tokens_normalized,
          COALESCE(top_tokens,'') AS top_tokens,
          COALESCE(CAST(eth_balance_deep AS REAL), NULL) AS eth_balance_deep,
          COALESCE(CAST(eth_usd_value_deep AS REAL), NULL) AS eth_usd_value_deep,
          COALESCE(CAST(total_token_balance_normalized_deep AS REAL), NULL) AS total_token_balance_normalized_deep,
          COALESCE(top_tokens_deep,'') AS top_tokens_deep,
          COALESCE(protocols_hint,'') AS protocols_hint,
          CASE WHEN COALESCE(CAST(distinct_tokens_ever AS INTEGER),0)=0
               THEN 0.0
               ELSE (1.0 - (CAST(distinct_tokens_ever AS REAL) / 20.0))
          END AS concentration_score,
          (
            LOG10(MAX(COALESCE(CAST(eth_usd_value AS REAL), COALESCE(CAST(eth_balance AS REAL),0.0)*1.0), 1.0)) * 40.0
            + MIN(20.0, COALESCE(CAST(tx_count_90d AS REAL),0.0)) * 1.5
            + MIN(10.0, COALESCE(CAST(token_actions_30d AS REAL),0.0)) * 1.0
            + CASE WHEN COALESCE(CAST(days_since_last_tx AS INTEGER),99999) <= 30 THEN 15.0 ELSE 0.0 END
            + CASE WHEN COALESCE(CAST(recv_from_exchange_count_lookback AS INTEGER),0) > 0 THEN 5.0 ELSE 0.0 END
            + MAX(0.0, COALESCE(CAST(total_in_tokens_normalized AS REAL),0.0) - COALESCE(CAST(total_out_tokens_normalized AS REAL),0.0)) * 0.01
            + (CASE WHEN COALESCE(CAST(distinct_tokens_ever AS INTEGER),0)=0 THEN 0.0 ELSE (1.0 - (CAST(distinct_tokens_ever AS REAL)/20.0)) END) * 5.0
          ) AS lead_score_candidate,
          '{RUN_ID}' AS run_id,
          '{PRODUCED_AT}' AS produced_at
        FROM raw;
    """)

    cols = [d[1] for d in cur.execute("PRAGMA table_info('final')")]
    with open(OUT, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(cols)
        for row in cur.execute("SELECT * FROM final ORDER BY lead_score_candidate DESC"):
            w.writerow(row)

    with open(SAMPLE, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(["addr_redacted","eth_usd_value","score","days_since_last_tx","top_tokens","protocols_hint"])
        for row in cur.execute("""
            SELECT
              UPPER(substr(address,1,8) || '...' || substr(address, length(address)-6, 6)) AS addr_redacted,
              ROUND(eth_usd_value,2),
              ROUND(lead_score_candidate,2),
              days_since_last_tx,
              top_tokens,
              protocols_hint
            FROM final
            ORDER BY lead_score_candidate DESC
            LIMIT 10
        """):
            w.writerow(row)

    total = cur.execute("SELECT COUNT(*) FROM final").fetchone()[0]
    take = min(500, total)
    with open(TOP_FOR_DEEP, 'w') as f:
        for (addr,) in cur.execute(f"""
            SELECT address FROM final
            ORDER BY lead_score_candidate DESC
            LIMIT {take}
        """):
            f.write(addr + "\n")

    readme = f"""Product: Whale Candidates CSV (one-off)\nRun ID: {RUN_ID}\nProduced At: {PRODUCED_AT}\n\nFiles:\n - final_whales.csv : full enriched list ordered by lead_score_candidate\n - redacted_top10.csv : redacted sample for outreach (addresses masked)\n - top500_for_deep.txt : addresses recommended for deep enrichment pass\n\nColumns: address, eth_balance, eth_usd_value, balance_tier, last_tx_ts, days_since_last_tx, tx_count_30d, tx_count_90d, tx_count_365d, token_actions_30d, token_actions_90d, distinct_tokens_ever, recv_from_exchange_count_lookback, send_to_exchange_count_lookback, is_contract, total_in_tokens_normalized, total_out_tokens_normalized, top_tokens, eth_balance_deep, eth_usd_value_deep, total_token_balance_normalized_deep, top_tokens_deep, protocols_hint, concentration_score, lead_score_candidate, run_id, produced_at\n\nScoring: size (log10 USD)*40 + capped activity + recency + exchange recv + net inflow + concentration bias.\n\nNotes:\n - Data sourced from public on-chain endpoints (Cloudflare RPC + Ethplorer free key).\n"""

    with open(README, 'w') as f:
        f.write(readme)

    with zipfile.ZipFile(ZIP, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(OUT, arcname=os.path.basename(OUT))
        zf.write(SAMPLE, arcname=os.path.basename(SAMPLE))
        zf.write(TOP_FOR_DEEP, arcname=os.path.basename(TOP_FOR_DEEP))
        zf.write(README, arcname=os.path.basename(README))

    print("WROTE:", OUT, SAMPLE, TOP_FOR_DEEP, README, ZIP)
    con.close()


if __name__ == '__main__':
    main()
