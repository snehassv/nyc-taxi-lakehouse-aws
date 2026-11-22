"""Copy the Glue job's load report from S3 and turn it into results/LOAD.md.

LOAD.md shows:
* what happened with the naive "read everything at once" approach
* every schema change the job made, file by file
* rows loaded per file next to the row count in the raw file's footer

Usage:
    python -m pipeline.load_report --bucket my-bucket
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parent.parent
RESULTS = Path(os.environ.get("TAXI_RESULTS_DIR", ROOT / "results"))


def to_markdown(load: dict, manifest: list[dict]) -> str:
    raw_rows = {m["file"]: m["rows"] for m in manifest}
    out = ["# Load report", ""]
    if load.get("failed"):
        return "\n".join(out + [f"Load FAILED: {load['error']}", ""])

    nc = load.get("naive_check")
    if nc:
        out += ["## The naive way: one Spark read over all files", ""]
        for mode, r in nc.items():
            if r["ok"]:
                extra = ""
                if "counts" in r:
                    missing = [c for c in ("cbd_congestion_fee", "airport_fee", "Airport_fee")
                               if c not in r["columns"]]
                    extra = f" Columns it did not see: {missing or 'none'}."
                out.append(f"- `{mode}`: ran ({r['seconds']}s), {len(r['columns'])} columns.{extra}")
            else:
                out.append(f"- `{mode}`: **failed** ({r['seconds']}s): `{r['error']}`")
        out.append("")

    out += ["## Schema changes made while loading", "",
            "| file | action | column | from | to |", "|---|---|---|---|---|"]
    for f in load["files"]:
        for c in f.get("changes", []):
            out.append(f"| {f['file']} | {c['action']} | {c['column']} | "
                       f"{c['from_type'] or ''} | {c['to_type'] or ''} |")

    out += ["", "## Row counts: raw file footer vs. Iceberg table", "",
            "| file | rows in raw file | rows in table | match |", "|---|---:|---:|:---:|"]
    total_raw = total_tbl = 0
    for f in load["files"]:
        if "rows_loaded" not in f:
            continue
        r, t = raw_rows.get(f["file"]), f["rows_loaded"]
        total_raw += r or 0
        total_tbl += t
        out.append(f"| {f['file']} | {r:,} | {t:,} | {'yes' if r == t else 'NO'} |")
    out.append(f"| **total** | **{total_raw:,}** | **{total_tbl:,}** | "
               f"{'yes' if total_raw == total_tbl else 'NO'} |")
    out += ["", f"Table totals: {load.get('table_rows')}", ""]
    return "\n".join(out)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", default=os.environ.get("TAXI_BUCKET"))
    a = p.parse_args()
    s3 = boto3.client("s3")
    load = json.loads(s3.get_object(Bucket=a.bucket, Key="results/load_report.json")["Body"].read())
    manifest = json.loads((RESULTS / "raw_manifest.json").read_text())
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "load_report.json").write_text(json.dumps(load, indent=2))
    (RESULTS / "LOAD.md").write_text(to_markdown(load, manifest))
    print(f"wrote {RESULTS / 'LOAD.md'}")


if __name__ == "__main__":
    main()
