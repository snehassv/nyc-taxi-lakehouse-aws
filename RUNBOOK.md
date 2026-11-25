# Runbook

Step-by-step notes for running this project from zero, then cleaning up.

## 0. One-time AWS setup

This project was built on an account made with AWS's **new sign-up experience**
(AWS Settings at <https://settings.aws.com>, "projects", Free plan with $100 credits).
In that experience there is no root user and no IAM access keys; your laptop gets
short-lived credentials with `aws login`. The project's Region is fixed by AWS
(US East (Ohio), `us-east-2`, for US addresses) - check it under
**AWS Settings -> your project -> Additional info**.

1. Turn on MFA for your sign-in (AWS Settings -> your name -> security / sign-in,
   or in your Google/GitHub/Apple account if you signed up with one).
2. Install the tools:
   ```bash
   brew install awscli                       # needs aws-cli 2.32+ for `aws login`
   brew tap hashicorp/tap && brew install hashicorp/tap/terraform
   ```
3. Log in (region `us-east-2`; say "no" to the MCP server question - not needed here):
   ```bash
   aws login --profile nyc-taxi
   aws sts get-caller-identity --profile nyc-taxi
   ```
   Credentials last 12 hours; run `aws login --profile nyc-taxi` again to renew.

If you used the classic sign-up instead, make an IAM user with access keys and
`aws configure --profile nyc-taxi`; everything else is the same (you can pick any Region
with `-var region=...`).

## 1. Billing alert first

```bash
make billing        # Terraform asks for alert_email
```
AWS sends a confirmation email for budget alerts only in some cases; check that the
budget shows up in **Billing → Budgets**.

## 2. Infrastructure

```bash
make infra          # review the plan, type "yes"
make env            # writes .env
```

## 3. Run the pipeline

```bash
make airflow-up     # first build takes a few minutes
make run            # or click "Trigger" in http://localhost:8080
```

The DAG has 4 tasks:

| task | what it does | where the output goes |
|---|---|---|
| `upload_raw_files` | downloads Jan of each year 2011–2025, reads each footer, uploads to S3 | `results/raw_manifest.json`, `results/raw_schema_diff.md` |
| `glue_load_iceberg` | runs the Glue job (naive-read test, then file-by-file load into both tables) | `s3://<bucket>/results/load_report.json` |
| `write_load_report` | copies the load report and makes a readable version | `results/LOAD.md` |
| `athena_benchmark` | runs 4 queries × 2 tables × 3 times; records data scanned and cost | `results/RESULTS.md`, `results/benchmark.json` |

To reload from scratch, trigger with `{"reset": "true"}`.

## 4. Check cost, then tear down

```bash
make destroy           # S3 bucket + data, Glue job + catalog, Athena workgroup, IAM role
```
Then in the console: **Billing → Bills** and **Cost Explorer** (data can lag ~24 hours).
When the bill looks right: `make destroy-billing`, then `aws logout --profile nyc-taxi`.
