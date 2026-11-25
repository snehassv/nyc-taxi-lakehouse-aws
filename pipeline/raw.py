"""Download NYC TLC yellow-taxi Parquet files and upload them to S3 (the raw layer).

For each year we take one month (January by default). Before uploading, we read
each file's Parquet footer (no full read) to record its row count and column
types. That gives:

* results/raw_manifest.json  - rows, bytes and columns per file
* results/raw_schema_diff.md - a table showing how columns changed by year

Usage:
    python -m pipeline.raw --bucket my-bucket --years 2011-2025 --month 1
"""
from __future__ import annotations

import argparse
import json
import os
import urllib.request
from pathlib import Path

import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{y}-{m:02d}.parquet"
RAW_PREFIX = "raw/yellow/"
CACHE = Path(os.environ.get("TAXI_CACHE_DIR", "data/raw"))
RESULTS = Path(os.environ.get("TAXI_RESULTS_DIR", "results"))


def parse_years(text: str) -> list[int]:
    if "-" in text:
        a, b = text.split("-")
        return list(range(int(a), int(b) + 1))
    return [int(x) for x in text.split(",")]


def check_available(url: str) -> None:
    """Stop early with a clear message if TLC moved or removed a file."""
    req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            if r.status != 200:
                raise RuntimeError(f"{url} returned HTTP {r.status}")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{url} is not available (HTTP {e.code})") from e


def download(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")
    with urllib.request.urlopen(url, timeout=120) as r, open(tmp, "wb") as f:
        while chunk := r.read(8 << 20):
            f.write(chunk)
    tmp.rename(dest)


def s3_exists(s3, bucket: str, key: str, size: int) -> bool:
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"] == size
    except ClientError:
        return False


def footer_info(path: Path) -> dict:
    meta = pq.ParquetFile(path)
    return {
        "rows": meta.metadata.num_rows,
        "columns": {f.name: str(f.type) for f in meta.schema_arrow},
    }


def schema_diff_markdown(manifest: list[dict]) -> str:
    """Columns (case-insensitive) x files, showing the type each file uses."""
    cols: dict[str, None] = {}
    for m in manifest:
        for c in m["columns"]:
            cols.setdefault(c.lower())
    header = "| column | " + " | ".join(m["file"][16:23] for m in manifest) + " |"
    lines = [header, "|" + "---|" * (len(manifest) + 1)]
    for c in cols:
        cells = []
        for m in manifest:
            hit = [(n, t) for n, t in m["columns"].items() if n.lower() == c]
            if not hit:
                cells.append("—")
            else:
                n, t = hit[0]
                cells.append(t if n == c else f"{t} (`{n}`)")
        lines.append(f"| {c} | " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n\n— = column not in that file. `Name` = different letter case.\n"


def run(bucket: str, years: list[int], month: int = 1) -> list[dict]:
    s3 = boto3.client("s3")
    manifest = []
    for y in years:
        url = URL.format(y=y, m=month)
        name = url.rsplit("/", 1)[-1]
        local = CACHE / name
        if not local.exists():
            check_available(url)
            print(f"downloading {name}")
            download(url, local)
        info = footer_info(local)
        size = local.stat().st_size
        key = RAW_PREFIX + name
        if not s3_exists(s3, bucket, key, size):
            print(f"uploading s3://{bucket}/{key} ({size / 1e6:.0f} MB)")
            s3.upload_file(str(local), bucket, key)
        manifest.append({"file": name, "bytes": size, **info})

    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "raw_manifest.json").write_text(json.dumps(manifest, indent=2))
    (RESULTS / "raw_schema_diff.md").write_text(schema_diff_markdown(manifest))
    s3.put_object(Bucket=bucket, Key="results/raw_manifest.json",
                  Body=json.dumps(manifest, indent=2).encode())
    total_rows = sum(m["rows"] for m in manifest)
    print(f"{len(manifest)} files, {total_rows:,} rows, "
          f"{sum(m['bytes'] for m in manifest) / 1e9:.2f} GB")
    return manifest


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", default=os.environ.get("TAXI_BUCKET"))
    p.add_argument("--years", default=os.environ.get("TAXI_YEARS", "2011-2025"))
    p.add_argument("--month", type=int, default=int(os.environ.get("TAXI_MONTH", "1")))
    a = p.parse_args()
    run(a.bucket, parse_years(a.years), a.month)
