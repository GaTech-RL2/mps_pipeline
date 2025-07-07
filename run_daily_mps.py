#!/usr/bin/env python3
"""
run_daily_mps.py   – ALWAYS processes every /mnt/raw/<folder>
  • normal call: launches Ray jobs immediately
  • --debug: dry-run (lists the folders, no work)
"""

import argparse
import datetime as dt
import os
import subprocess
import sys
import traceback
from pathlib import Path

import ray

RAW_ROOT = Path("/mnt/raw")
TOKEN = (
    "FRLAbl7Eqw5g4upZCoow1ht3YE9e16ue9iLTv13IpnXXxxt8gR5BXrqkuj6deunEcnDAUMNjylAZAv"
    "SKzZB6PB2amlGFec8dyuvpaZAtZB0hxxzRRHgoy9gdZChM8lUDalGDP1q8VPoszMZBLiYoif0Q9aL49"
    "Ewn0mXUEVd3gHBW74yAwZD"
)

def ensure_token():
    dst = Path.home() / ".projectaria" / "auth_token"
    if not dst.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(TOKEN)

@ray.remote(num_cpus=8, memory=32 * 1024 ** 3)
def run_mps_on_folder(folder: str) -> dict:
    try:
        ensure_token()
        subprocess.run(
            ["aria_mps", "single", "-i", folder, "--no-ui", "--retry-failed"],
            check=True,
        )
        return {"folder": folder, "status": "ok"}
    except Exception as exc:
        return {"folder": folder, "status": "err", "err": str(exc),
                "trace": traceback.format_exc(limit=3)}

def discover_subfolders() -> list[str]:
    if not RAW_ROOT.exists():
        raise RuntimeError(f"{RAW_ROOT} does not exist or is not mounted?")
    return [str(p) for p in RAW_ROOT.iterdir() if p.is_dir()]

def launch_jobs(folders: list[str]) -> None:
    print(f"Launching {len(folders)} parallel MPS jobs …")
    futures = [run_mps_on_folder.remote(f) for f in folders]
    pending = set(futures)
    while pending:
        ready, pending = ray.wait(list(pending), num_returns=1)
        res = ray.get(ready[0])
        ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if res["status"] == "ok":
            print(f"[{ts}] ✓ {res['folder']}")
        else:
            print(f"[{ts}] ✗ {res['folder']} :: {res['err']}")
            print(res["trace"])

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true",
                        help="Dry-run: list folders only, no work")
    args = parser.parse_args()

    folders = discover_subfolders()

    if args.debug:                       # ===== DRY-RUN =====
        print("DEBUG MODE – nothing will run")
        for f in folders:
            print(" would process:", f)
        sys.exit(0)

    # ===== REAL RUN =====
    ray.init(address="auto")
    launch_jobs(folders)

if __name__ == "__main__":
    main()
