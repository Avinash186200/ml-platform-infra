# Contributing

Changes are judged on whether they keep the point-in-time guarantee, keep CI offline and credential-free, and keep the numbers honest.

## Ground rules

1. **No cloud credentials, ever, in CI.** Terraform is checked with `init -backend=false`, `validate`, tflint and checkov. Nothing plans or applies. If a check needs an account, it does not belong in CI.
2. **No secrets in the repo.** Kubernetes manifests reference the `redis-auth` Secret but never contain one; `terraform.tfvars`, state and `.env` are git-ignored; gitleaks runs on every push.
3. **Point in time is not optional.** Any new feature must set `available_at` to the publication time of its newest input and pass the truncation test in `tests/test_features.py`. The as-of rule lives in `pit.py`; the SQL in `store.py` and the vectorised join must keep agreeing with `asof_bruteforce` (property tests in `tests/test_pit.py`).
4. **Numbers come from results.** Any number in README or docs must be a field in `results/*.json` written by `make bench`. CI reruns the bench and fails if the fresh run differs from the committed JSON, or if `RESULTS.md` is not rendered from it.
5. **No timing-dependent tests.** Refresh intervals and job leases take an injected clock; tests never sleep. The bench records no timings.
6. **The gate is not a knob.** Do not loosen it to get a model promoted. If the gate rules change, say why in the changelog and rerun the control mode to show it still blocks a no-signal model.

## Set up

```bash
python -m venv .venv && source .venv/bin/activate
pip install -c constraints.txt -e ".[dev]"
pytest -q -m "not slow"   # about 25 s
pytest -q                 # adds the full bench rerun, about a minute
make tf-check             # needs terraform and tflint
make k8s-check            # needs kustomize and kubeconform
```

## Changing infrastructure

- Run `terraform fmt -recursive` and `make tf-check`. Every variable and output needs a description (tflint enforces it).
- A new checkov finding is fixed, or skipped inline next to the resource with a reason a reviewer can disagree with. Add the skip to the table in `docs/cost-and-security.md`.
- New IAM permissions name exact actions and resources; `tests/test_manifests.py` rejects wildcards and deletes in the workload policies.

## Changing manifests

- Every container keeps requests and limits, a non-root read-only security context and (for HTTP services) `/readyz` and `/healthz` probes; `tests/test_manifests.py` checks this.
- A new kind needs its schema added to `scripts/fetch-k8s-schemas.sh`; kubeconform treats a missing schema as an error on purpose.
