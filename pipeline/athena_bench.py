"""Run the benchmark queries on both Iceberg tables and record data scanned and cost.

For every query in sql/benchmark_queries.sql we run it on trips_unpartitioned and
trips_partitioned (3 runs each), read Athena's own statistics, and check that
both tables return the same answer.

Cost uses Athena's on-demand SQL price: $5 per TB scanned, rounded up to the
nearest MB, with a 10 MB minimum per query. We treat 1 TB = 1024^4 bytes
(1 MB = 1024^2). The price is a constant below so it is easy to check.

It also records:
* row counts per source file in each table (to compare with the raw manifest)
* data files and bytes in each table (from Iceberg's $files metadata table)
* how many rows have a value in each "new" column, per year (schema evolution proof)

Usage:
    python -m pipeline.athena_bench --workgroup W --database D
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import time
from pathlib import Path

import boto3

PRICE_PER_TB = 5.0
MB = 1024 ** 2
TB = 1024 ** 4
TABLES = ["trips_unpartitioned", "trips_partitioned"]
RUNS = 3
ROOT = Path(__file__).resolve().parent.parent
RESULTS = Path(os.environ.get("TAXI_RESULTS_DIR", ROOT / "results"))


def query_cost(bytes_scanned: int) -> float:
    billed_mb = max(10, math.ceil(bytes_scanned / MB))
    return billed_mb * MB / TB * PRICE_PER_TB


def load_queries(path: Path) -> list[dict]:
    text = path.read_text()
    out = []
    for block in re.split(r"(?m)^-- name: ", text)[1:]:
        head, sql = block.split("\n", 1)
        qid, desc = [s.strip() for s in head.split("|", 1)]
        sql = "\n".join(l for l in sql.splitlines() if not l.startswith("--")).strip().rstrip(";")
        out.append({"id": qid, "description": desc, "sql": sql})
    return out


class Athena:
    def __init__(self, workgroup: str, database: str):
        self.c = boto3.client("athena")
        self.wg, self.db = workgroup, database

    def run(self, sql: str) -> dict:
        qid = self.c.start_query_execution(
            QueryString=sql,
            WorkGroup=self.wg,
            QueryExecutionContext={"Database": self.db},
            ResultReuseConfiguration={"ResultReuseByAgeConfiguration": {"Enabled": False}},
        )["QueryExecutionId"]
        while True:
            q = self.c.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = q["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)
        if state != "SUCCEEDED":
            raise RuntimeError(f"Athena query {state}: {q['Status'].get('StateChangeReason')}\n{sql}")
        rows = []
        for page in self.c.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
            for r in page["ResultSet"]["Rows"]:
                rows.append([d.get("VarCharValue") for d in r["Data"]])
        st = q["Statistics"]
        return {
            "query_id": qid,
            "bytes_scanned": st.get("DataScannedInBytes", 0),
            "engine_ms": st.get("EngineExecutionTimeInMillis"),
            "rows": rows,  # first row is the header
        }


def benchmark(ath: Athena, queries: list[dict]) -> list[dict]:
    results = []
    for q in queries:
        entry = {"id": q["id"], "description": q["description"], "tables": {}}
        for t in TABLES:
            runs = [ath.run(q["sql"].format(table=t)) for _ in range(RUNS)]
            scanned = runs[0]["bytes_scanned"]
            entry["tables"][t] = {
                "bytes_scanned": scanned,
                "bytes_scanned_all_runs": [r["bytes_scanned"] for r in runs],
                "cost_usd": query_cost(scanned),
                "median_engine_ms": statistics.median(r["engine_ms"] for r in runs),
                "answer": runs[0]["rows"],
                "query_id": runs[0]["query_id"],
            }
            print(f"{q['id']:<22} {t:<20} {scanned / MB:>10.1f} MB")
        a, b = (entry["tables"][t]["answer"] for t in TABLES)
        entry["same_answer"] = a == b
        u, p = (entry["tables"][t]["bytes_scanned"] for t in TABLES)
        entry["scan_reduction_pct"] = round(100 * (1 - p / u), 1) if u else None
        results.append(entry)
    return results


def table_facts(ath: Athena) -> dict:
    facts = {}
    for t in TABLES:
        files = ath.run(
            f'SELECT count(*), sum(file_size_in_bytes), sum(record_count) FROM "{t}$files"'
        )["rows"][1]
        per_file = ath.run(
            f"SELECT source_file, count(*) FROM {t} GROUP BY 1 ORDER BY 1"
        )["rows"][1:]
        facts[t] = {
            "data_files": int(files[0]),
            "data_bytes": int(files[1]),
            "rows": int(files[2]),
            "rows_per_source_file": {f: int(n) for f, n in per_file},
        }
    if any(f["rows"] != facts[TABLES[0]]["rows"] for f in facts.values()):
        print("WARNING: the two tables have different row counts")
    return facts


def evolution_proof(ath: Athena) -> list[list]:
    sql = """
    SELECT source_file,
           count(*) AS rows,
           count(congestion_surcharge) AS has_congestion_surcharge,
           count(airport_fee) AS has_airport_fee,
           count(cbd_congestion_fee) AS has_cbd_congestion_fee
    FROM trips_unpartitioned
    GROUP BY 1 ORDER BY 1"""
    return ath.run(sql)["rows"]


def to_markdown(report: dict) -> str:
    lines = ["# Benchmark results", "",
             f"Run at {report['run_at']} UTC, Athena workgroup `{report['workgroup']}`, "
             f"{RUNS} runs per query (data scanned is the same every run).", "",
             "## Data scanned and cost per query", "",
             "| query | not partitioned | partitioned | less data scanned | cost not partitioned | cost partitioned | same answer |",
             "|---|---:|---:|---:|---:|---:|:---:|"]
    for r in report["benchmark"]:
        u, p = (r["tables"][t] for t in TABLES)
        lines.append(
            f"| {r['id']}: {r['description']} | {u['bytes_scanned'] / MB:,.1f} MB | "
            f"{p['bytes_scanned'] / MB:,.1f} MB | {r['scan_reduction_pct']}% | "
            f"${u['cost_usd']:.6f} | ${p['cost_usd']:.6f} | {'yes' if r['same_answer'] else 'NO'} |")
    lines += ["", "## Tables", "", "| table | rows | data files | size |", "|---|---:|---:|---:|"]
    for t, f in report["tables"].items():
        lines.append(f"| {t} | {f['rows']:,} | {f['data_files']:,} | {f['data_bytes'] / MB:,.1f} MB |")
    lines += ["", "## New columns by file (schema evolution)", ""]
    ev = report["evolution"]
    lines.append("| " + " | ".join(ev[0]) + " |")
    lines.append("|---" + "|---:" * (len(ev[0]) - 1) + "|")
    for row in ev[1:]:
        lines.append("| " + " | ".join(f"{int(v):,}" if v and v.isdigit() else str(v) for v in row) + " |")
    return "\n".join(lines) + "\n"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--workgroup", default=os.environ.get("TAXI_ATHENA_WORKGROUP"))
    p.add_argument("--database", default=os.environ.get("TAXI_DATABASE"))
    a = p.parse_args()
    ath = Athena(a.workgroup, a.database)
    report = {
        "run_at": time.strftime("%Y-%m-%d %H:%M", time.gmtime()),
        "workgroup": a.workgroup,
        "price_per_tb_usd": PRICE_PER_TB,
        "benchmark": benchmark(ath, load_queries(ROOT / "sql" / "benchmark_queries.sql")),
        "tables": table_facts(ath),
        "evolution": evolution_proof(ath),
    }
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "benchmark.json").write_text(json.dumps(report, indent=2))
    (RESULTS / "RESULTS.md").write_text(to_markdown(report))
    print(f"wrote {RESULTS / 'RESULTS.md'}")


if __name__ == "__main__":
    main()
