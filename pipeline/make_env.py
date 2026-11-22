"""Write .env (used by Airflow) from `terraform output -json` on stdin."""
import json
import sys

o = {k: v["value"] for k, v in json.load(sys.stdin).items()}
print("\n".join([
    f"TAXI_BUCKET={o['bucket']}",
    f"TAXI_DATABASE={o['database']}",
    f"TAXI_GLUE_JOB={o['glue_job']}",
    f"TAXI_ATHENA_WORKGROUP={o['athena_workgroup']}",
    f"AWS_DEFAULT_REGION={o['region']}",
    "TAXI_YEARS=2011-2025",
    "TAXI_MONTH=1",
]))
