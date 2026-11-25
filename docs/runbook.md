# Runbook

Written for the dev environment. None of these steps has been run against a real AWS account (see the README); they are the intended procedure, and each names how to check it worked.

Tools: `aws` CLI v2, `terraform` >= 1.6, `kubectl`, `kustomize`, `docker`. Commands assume the repo root and an assumed deploy role with the permissions to create the resources in `infra/terraform`.

## 1. First deploy

```bash
# Infrastructure
cd infra/terraform/envs/dev
cp terraform.tfvars.example terraform.tfvars      # set api_allowed_cidrs and cluster_admin_role_arns
terraform init                                     # add the S3 backend first if you want remote state (backend.tf)
terraform plan -out tfplan                         # read it: IAM, security groups, public endpoint CIDRs
terraform apply tfplan
terraform output                                   # bucket, repository, Redis endpoint, role ARNs

# Kubeconfig (the admin role must be in cluster_admin_role_arns)
aws eks update-kubeconfig --name "$(terraform output -raw cluster_name)"

# Image: tags are immutable, so every build gets a new tag
cd ../../../..
REPO=$(terraform -chdir=infra/terraform/envs/dev output -raw image_repository)
TAG=1.0.0
aws ecr get-login-password | docker login --username AWS --password-stdin "${REPO%%/*}"
docker build -t "$REPO:$TAG" . && docker push "$REPO:$TAG"
```

Fill the dev overlay from the Terraform outputs (account id, bucket, Redis endpoint, role ARNs) in `k8s/overlays/dev/kustomization.yaml` and `serviceaccount-roles.yaml`, and set the image tag:

```bash
cd k8s/overlays/dev && kustomize edit set image mlplatform="$REPO:$TAG" && cd -
```

Create the Redis secret from Secrets Manager (the value never touches the repo or the shell history):

```bash
SECRET_ARN=$(terraform -chdir=infra/terraform/envs/dev output -raw redis_auth_secret_arn)
kubectl create namespace ml-platform --dry-run=client -o yaml | kubectl apply -f -
aws secretsmanager get-secret-value --secret-id "$SECRET_ARN" --query SecretString --output text \
  | kubectl -n ml-platform create secret generic redis-auth --from-file=password=/dev/stdin \
      --dry-run=client -o yaml | kubectl apply -f -
```

Deploy and check:

```bash
make k8s-check                                   # same validation as CI
kubectl apply -k k8s/overlays/dev
kubectl -n ml-platform rollout status deploy/feature-api deploy/model-api deploy/jobs-worker
```

The feature API stays unready until a snapshot exists. Run the Dagster jobs once (via the Dagster deployment, or locally with `MLPI_ARTIFACTS_URL=s3://<bucket>` and the deploy role): `nightly_features_job`, then `weekly_retrain_job`, then `promote_model_job`. Then:

```bash
kubectl -n ml-platform port-forward svc/feature-api 8000:80 &
curl "http://127.0.0.1:8000/readyz"                       # {"status":"ready","snapshot_version":1}
curl "http://127.0.0.1:8000/features/SYN001?as_of=2024-06-03T22:00:00Z"
curl "http://127.0.0.1:8000/stats"                        # cache errors should be 0
```

## 2. Routine deploy (new image)

1. Merge to main; CI must be green (tests, terraform, kubernetes, checkov, image, secrets).
2. Build and push a new tag (never reuse a tag; ECR rejects it anyway).
3. `kustomize edit set image mlplatform=$REPO:$NEW_TAG` in the overlay, commit, `kubectl apply -k k8s/overlays/dev`.
4. `kubectl rollout status`. Rolling updates use `maxUnavailable: 0`, so the old pods serve until new ones pass `/readyz`.

## 3. Rollback

There are three independent things to roll back; pick the one that broke.

**Application image.**

```bash
kubectl -n ml-platform rollout undo deploy/feature-api      # or set the previous tag and apply
kubectl -n ml-platform rollout status deploy/feature-api
```

Then revert the image tag in the overlay so the repo matches the cluster.

**Feature snapshot** (a bad nightly build that passed its checks). Point `features/CURRENT` back to the previous version; pods pick it up within `MLPI_REFRESH_SECONDS` and the previous version's cache entries become valid again.

```bash
aws s3 cp s3://<bucket>/features/CURRENT - | jq .        # version, sha256, previous, previous_key, previous_sha256
# Build the new pointer first; jq -e stops here (and nothing is written) if there is no previous version.
NEW=$(aws s3 cp s3://<bucket>/features/CURRENT - \
  | jq -e 'if .previous == null then error("no previous snapshot to roll back to") else . end
           | {version: .previous, key: .previous_key, sha256: .previous_sha256,
              previous: .version, previous_key: .key, previous_sha256: .sha256}') \
  && echo "$NEW" | aws s3 cp - s3://<bucket>/features/CURRENT
curl http://127.0.0.1:8000/readyz                         # the previous snapshot_version
```

The next nightly run numbers its snapshot after the highest file in the bucket, not after `CURRENT`, so it can never overwrite the bad version (kept for the post-mortem). Stop `nightly_features_schedule` until the cause is fixed, or that run publishes on top of your rollback.

**Pruning old snapshots.** There is deliberately no S3 expiry on `features/v*`. With the deploy role, delete files older than you need, never the ones `features/CURRENT` (`key`, `previous_key`) or `features/STAGED` name:

```bash
aws s3 cp s3://<bucket>/features/CURRENT - | jq -r '.key, .previous_key'   # keep these
aws s3 ls s3://<bucket>/features/                                          # pick the rest by version
```

Versioning keeps a deleted file recoverable for 30 days (`noncurrent_version_days`).

**Model.** `models/CURRENT` records `previous` and `previous_key`. Write those back:

```bash
NEW=$(aws s3 cp s3://<bucket>/models/CURRENT - \
  | jq -e 'if .previous == null then error("no previous model to roll back to") else . end
           | {model_id: .previous, key: .previous_key, previous: .model_id, previous_key: .key}') \
  && echo "$NEW" | aws s3 cp - s3://<bucket>/models/CURRENT
kubectl -n ml-platform port-forward svc/model-api 8001:80 & curl http://127.0.0.1:8001/readyz
```

Turn off the `promote_on_approval` sensor so the next weekly run does not re-promote automatically.

**Infrastructure.** Revert the Terraform change in git, `terraform plan`, read it, `terraform apply`. Never `terraform destroy` to undo a change. The bucket has `force_destroy = false` and versioning, so even a mistaken destroy cannot silently delete artifacts.

## 4. Rotate secrets

**Redis AUTH token.** ElastiCache supports a two-phase change, so connected pods never lose access:

1. Generate a new token and set it with strategy `ROTATE` (old and new both valid):
   ```bash
   NEW=$(openssl rand -hex 24)
   aws elasticache modify-replication-group --replication-group-id mlplat-dev-redis \
     --auth-token "$NEW" --auth-token-update-strategy ROTATE --apply-immediately
   ```
2. Store it: `aws secretsmanager put-secret-value --secret-id <arn> --secret-string "$NEW"`.
3. Re-create the `redis-auth` Kubernetes Secret (command in section 1) and restart the readers: `kubectl -n ml-platform rollout restart deploy/feature-api deploy/jobs-worker`. The token is read from the mounted file at start-up.
4. Once all pods run with the new token, set it again with strategy `SET` to drop the old one.
5. Terraform's `random_password` still holds the old value in state. Either add `lifecycle { ignore_changes = [auth_token] }` to the replication group before the first rotation, or taint `random_password.auth` and let Terraform drive the change (it applies the new token; follow with steps 2-3).

**IAM.** There are no long-lived keys to rotate: pods use IRSA web-identity tokens (refreshed automatically), CI has no cloud credentials. If an IRSA role is suspected compromised, change its trust policy (`modules/iam`) or delete and re-create the role; tokens already issued expire within the session duration (1 hour).

**KMS keys** rotate automatically every year (`enable_key_rotation = true`); nothing to do.

## 5. Add a feature

1. **Compute it point in time.** Add the column in `features.price_feature_frame` (or a new record source in `build_feature_records`) using only data published by the row's time, and set `available_at` to the publication time of the newest input. Add its name to `PRICE_FEATURES` / `ALL_FEATURES`.
2. **Test it.** Extend `tests/test_features.py`: a hand-computed value, and the truncation test must still pass (history after day D does not change rows up to D). The property tests in `test_pit.py` cover the as-of lookup for any feature.
3. **Decide if the model uses it.** Only then add it to `MODEL_FEATURES`. The model API will start asking for it as soon as a model trained on it is promoted, and returns 422 if it is missing for a request.
4. **Rebuild the results.** `make bench`, read the diff in `results/RESULTS.md`, commit the JSON with the code. CI fails if they disagree.
5. **Roll out.** Deploy the image, run `nightly_features_job` (new snapshot with the feature), then `weekly_retrain_job`. The API serves the new feature from the first snapshot that contains it; older snapshots simply do not have it, and the API rejects unknown feature names with 400.

## 6. Day-two checks

| signal | where | action |
|---|---|---|
| feature API unready | `/readyz` 503 | no snapshot loaded: check `features/CURRENT`, the pod's IRSA role, `/stats.snapshot_refresh_errors` |
| cache errors rising | `/stats.cache.errors` | Redis reachability or the AUTH secret; requests still succeed (fail-open) |
| jobs piling up | `/stats.jobs.queued` | worker pods running? `kubectl logs deploy/jobs-worker` |
| job stuck in `failed` | `GET /jobs/{key}` → `last_error` | fix the cause, resubmit with a new key (the old key stays `failed` for 7 days on purpose) |
| new snapshot not served | `features/STAGED` newer than `features/CURRENT` and `features/CHECKED` | the point-in-time check failed on it: read the Dagster run, fix, rerun `nightly_features_job`. Running `publish_feature_snapshot` by hand will refuse, by design |
| gate keeps failing | Dagster run logs, `models/candidates/<id>/evaluation.json` | expected if the data has no signal; do not loosen the gate to force a promotion |
