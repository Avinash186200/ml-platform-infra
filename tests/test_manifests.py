"""House rules for the Kubernetes manifests and Terraform, checked without any binaries.

CI also renders the manifests with kustomize and validates them with
kubeconform, and runs terraform validate, tflint and checkov; these tests
encode the rules those tools do not know about.
"""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "k8s" / "base"
TF = ROOT / "infra" / "terraform"


def docs():
    out = []
    for f in sorted(BASE.glob("*.yaml")):
        if f.name == "kustomization.yaml":
            continue
        out += [(f.name, d) for d in yaml.safe_load_all(f.read_text(encoding="utf-8")) if d]
    return out


def pod_specs():
    for name, d in docs():
        if d["kind"] == "Deployment":
            yield name, d, d["spec"]["template"]["spec"]
        elif d["kind"] == "CronJob":
            yield name, d, d["spec"]["jobTemplate"]["spec"]["template"]["spec"]


def test_kustomization_lists_every_file():
    k = yaml.safe_load((BASE / "kustomization.yaml").read_text(encoding="utf-8"))
    files = {f.name for f in BASE.glob("*.yaml")} - {"kustomization.yaml"}
    assert set(k["resources"]) == files


def test_no_secret_objects_or_values_in_the_repo():
    assert not [d for _, d in docs() if d["kind"] == "Secret"]
    for f in (ROOT / "k8s").rglob("*.yaml"):
        text = f.read_text(encoding="utf-8").lower()
        assert "password:" not in text.replace("key: password", ""), f
    cm = next(d for _, d in docs() if d["kind"] == "ConfigMap")
    assert not any("PASSWORD" in k or "TOKEN" in k or "SECRET" in k for k in cm["data"])


@pytest.mark.parametrize("name,doc,spec", list(pod_specs()), ids=lambda x: x if isinstance(x, str) else "")
def test_every_container_is_locked_down_and_sized(name, doc, spec):
    assert spec["securityContext"]["runAsNonRoot"] is True
    assert spec["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
    assert spec.get("automountServiceAccountToken") is False
    for c in spec["containers"]:
        sc = c["securityContext"]
        assert sc["allowPrivilegeEscalation"] is False
        assert sc["readOnlyRootFilesystem"] is True
        assert sc["capabilities"]["drop"] == ["ALL"]
        for kind in ("requests", "limits"):
            assert {"cpu", "memory"} <= set(c["resources"][kind]), (name, c["name"], kind)
        assert ":" not in c["image"] and c["image"] == "mlplatform"  # tag set once, by kustomize


def test_http_services_have_probes_hpa_and_pdb():
    all_docs = [d for _, d in docs()]
    deployments = [d for d in all_docs if d["kind"] == "Deployment"]
    for d in deployments:
        c = d["spec"]["template"]["spec"]["containers"][0]
        assert "livenessProbe" in c, d["metadata"]["name"]
        if c.get("ports"):  # the HTTP APIs
            assert c["readinessProbe"]["httpGet"]["path"] == "/readyz"
            assert c["livenessProbe"]["httpGet"]["path"] == "/healthz"
            name = d["metadata"]["name"]
            kinds = {x["kind"] for x in all_docs if x["metadata"]["name"] == name}
            assert {"Service", "HorizontalPodAutoscaler", "PodDisruptionBudget"} <= kinds, name


def test_redis_password_comes_from_a_mounted_secret():
    users = 0
    for _, _, spec in pod_specs():
        for c in spec["containers"]:
            env = {e["name"]: e for e in c.get("env", [])}
            assert "MLPI_REDIS_PASSWORD" not in env  # never as a plain env var
            if "MLPI_REDIS_PASSWORD_FILE" in env:
                users += 1
                vols = {v["name"]: v for v in spec["volumes"]}
                assert vols["redis-auth"]["secret"]["secretName"] == "redis-auth"
    assert users == 2  # feature-api and jobs-worker


def test_cronjob_cannot_pile_up_and_uses_an_idempotent_key():
    cj = next(d for _, d in docs() if d["kind"] == "CronJob")
    assert cj["spec"]["concurrencyPolicy"] == "Forbid"
    assert cj["spec"]["timeZone"] == "Etc/UTC"
    args = " ".join(cj["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["args"])
    assert '--job-key "nightly-export-$(date -u +%Y-%m-%d)"' in args


def test_service_accounts_match_terraform_workloads():
    sas = {d["metadata"]["name"] for _, d in docs() if d["kind"] == "ServiceAccount"}
    main = (TF / "envs" / "dev" / "main.tf").read_text(encoding="utf-8")
    tf_sas = set(re.findall(r'service_account\s*=\s*"([^"]+)"', main))
    assert sas <= tf_sas  # every pod identity has an IAM role (dagster's runs outside these manifests)
    overlay = (ROOT / "k8s" / "overlays" / "dev" / "serviceaccount-roles.yaml").read_text(encoding="utf-8")
    for sa in sas:
        assert f"role/mlplat-dev-{sa}" in overlay


def test_iam_grants_no_wildcard_actions_and_no_deletes():
    text = (TF / "modules" / "iam" / "main.tf").read_text(encoding="utf-8")
    actions = re.findall(r"actions\s*=\s*(.+)", text)
    assert actions
    for a in actions:
        assert '"*"' not in a and "s3:*" not in a and "Delete" not in a, a


def test_no_real_tfvars_or_state_committed():
    assert not list(TF.rglob("*.tfstate*"))
    assert not [p for p in TF.rglob("*.tfvars") if p.name != "terraform.tfvars.example"]


def test_network_policies_deny_by_default_both_ways_and_scope_redis():
    pols = {d["metadata"]["name"]: d for _, d in docs() if d["kind"] == "NetworkPolicy"}
    for name, kind in [("default-deny-ingress", "Ingress"), ("default-deny-egress", "Egress")]:
        assert pols[name]["spec"]["podSelector"] == {} and pols[name]["spec"]["policyTypes"] == [kind]
        assert kind.lower() not in pols[name]["spec"]
    redis_users = pols["egress-redis"]["spec"]["podSelector"]["matchExpressions"][0]["values"]
    assert sorted(redis_users) == ["feature-api", "jobs-worker"]  # the model API cannot reach Redis
    https = pols["egress-https-aws"]["spec"]["egress"][0]["to"][0]["ipBlock"]
    assert "169.254.169.254/32" in https["except"]  # no instance metadata from pods
    for p in pols.values():  # every egress rule names a port
        for rule in p["spec"].get("egress", []):
            assert rule["ports"], p["metadata"]["name"]


def test_terraform_enables_network_policy_enforcement():
    text = (TF / "modules" / "eks" / "main.tf").read_text(encoding="utf-8")
    assert 'addon_name                  = "vpc-cni"' in text
    assert 'enableNetworkPolicy = "true"' in text


def test_worker_liveness_checks_its_heartbeat_file():
    w = next(d for _, d in docs() if d["kind"] == "Deployment" and d["metadata"]["name"] == "jobs-worker")
    c = w["spec"]["template"]["spec"]["containers"][0]
    assert c["livenessProbe"]["exec"]["command"][:2] == ["mlplatform", "worker-alive"]
    env = {e["name"]: e.get("value") for e in c["env"]}
    assert env["MLPI_WORKER_ALIVE_FILE"] in c["livenessProbe"]["exec"]["command"]
