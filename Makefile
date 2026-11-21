# Run from the repo root. Needs: terraform, aws CLI, docker, python 3.11+.
# AWS_PROFILE is the CLI profile you made with `aws configure --profile nyc-taxi`.
AWS_PROFILE ?= nyc-taxi
export AWS_PROFILE

.PHONY: test billing infra env airflow-up run results destroy destroy-billing

test:                      ## unit tests (no AWS needed)
	python -m pytest -q

billing:                   ## step 1: budget alert (asks for your email)
	cd infra/billing && terraform init -input=false && terraform apply

infra:                     ## step 2: S3, Glue, Athena, IAM
	cd infra/main && terraform init -input=false && terraform apply

env:                       ## write .env for Airflow from Terraform outputs
	terraform -chdir=infra/main output -json | python3 pipeline/make_env.py > .env
	@cat .env

airflow-up:                ## start local Airflow at http://localhost:8080
	cd airflow && docker compose up -d --build

run:                       ## trigger the DAG
	cd airflow && docker compose exec airflow airflow dags unpause nyc_taxi_lakehouse && \
	docker compose exec airflow airflow dags trigger nyc_taxi_lakehouse

destroy:                   ## delete ALL AWS resources and data for this project
	cd airflow && docker compose down -v || true
	cd infra/main && terraform destroy

destroy-billing:           ## remove the budget too (do this last, after checking the bill)
	cd infra/billing && terraform destroy
