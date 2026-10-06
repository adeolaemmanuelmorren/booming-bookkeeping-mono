"""Read-only comparison. Run after secrets are provisioned and Worker routes deployed."""
import argparse
import collections
import json
import subprocess
import urllib.request
from concurrent.futures import ThreadPoolExecutor

parser = argparse.ArgumentParser()
parser.add_argument("--start", required=True)
parser.add_argument("--end", required=True)
args = parser.parse_args()
project = "able-folio-499722"
worker = "https://bill-realtime-ondemand-source-d7ef.bill-3e3.workers.dev"

def gcloud(*args):
    return subprocess.check_output(["gcloud", *args], text=True).strip()

def request(url, headers, data=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=180) as response:
        return json.load(response)

access = gcloud("auth", "print-access-token")
identity = gcloud("auth", "print-identity-token")
secret = gcloud("secrets", "versions", "access", "latest",
                "--project=" + project, "--secret=bill-ondemand-source-token")
google = request(
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
    "google-ads-reader@able-folio-499722.iam.gserviceaccount.com:generateAccessToken",
    {"Authorization": "Bearer " + access, "Content-Type": "application/json"},
    {"scope": ["https://www.googleapis.com/auth/adwords"], "lifetime": "600s"},
)["accessToken"]

def normalize(value):
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()
                if k not in ("fetchedAt", "_dataform_updated_at")}
    if isinstance(value, list):
        return sorted((normalize(v) for v in value), key=lambda v: json.dumps(v, sort_keys=True))
    return value

routes = [
    ("google", "performance"), ("google", "hourly-performance"),
    ("meta", "performance"), ("meta", "hourly-performance"),
    ("meta", "delivery"), ("meta", "dashboard"),
]
results = []
for provider, endpoint in routes:
    path = f"/v1/{provider}-ads/{endpoint}?start_date={args.start}&end_date={args.end}"
    old = ("https://google-ads-realtime-sync-943586541190.us-central1.run.app" if provider == "google"
           else "https://meta-ads-query-api-lbzab5q2qq-uc.a.run.app")
    with ThreadPoolExecutor(max_workers=2) as pool:
        left = pool.submit(request, old + path, {"Authorization": "Bearer " + identity})
        right = pool.submit(request, worker + path, {
            "Authorization": "Bearer " + secret, "X-Google-Ads-Access-Token": google,
        })
        old_body, new_body = left.result(), right.result()
    summary = {"route": f"{provider}/{endpoint}", "equal": normalize(old_body) == normalize(new_body),
               "oldRows": len(old_body.get("rows", old_body.get("hourlyRows", []))),
               "workerRows": len(new_body.get("rows", new_body.get("hourlyRows", [])))}
    results.append(summary)
    print(json.dumps(summary), flush=True)
if not all(r["equal"] and r["workerRows"] > 0 for r in results):
    raise SystemExit("Live parity has not passed. Do not switch callers.")
