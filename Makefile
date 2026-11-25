.PHONY: install test test-fast bench report demo tf-check k8s-check up

install:
	pip install -c constraints.txt -e ".[dev]"

test:
	pytest -q

# Everything except the full bench rerun.
test-fast:
	pytest -q -m "not slow"

# Dagster graph (signal + control), leakage contrast, PIT audit, cache replay -> results/*.json, RESULTS.md
bench:
	python -m mlplatform.cli bench --out results

report:
	python -m mlplatform.cli report --out results

# Run the asset graph once into data/artifacts and promote the model.
demo:
	python -m mlplatform.cli demo --artifacts data/artifacts

# What CI runs for Terraform (needs terraform and tflint; no cloud credentials).
tf-check:
	cd infra/terraform && terraform fmt -check -recursive
	cd infra/terraform/envs/dev && terraform init -backend=false -input=false && terraform validate
	cd infra/terraform && tflint --init --config "$$PWD/.tflint.hcl" && tflint --recursive --config "$$PWD/.tflint.hcl"

# What CI runs for Kubernetes (needs kustomize and kubeconform).
k8s-check:
	bash scripts/fetch-k8s-schemas.sh
	bash scripts/validate-k8s.sh

up:
	docker compose up --build
