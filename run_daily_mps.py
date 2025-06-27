#!/usr/bin/env python3
"""
run_daily_mps.py
----------------
  • Scans every immediate sub-directory of /mnt/raw
  • Starts one parallel Ray task per folder:
        aria_mps single -i <folder> --no-ui --retry-failed
  • Designed to be launched once per night from cron, BUT
    you can run it anytime with --debug to trigger work immediately.

Environment prerequisites (already in your Ray AMI/user-data):
  * mount-s3 has mounted s3://rldb/raw → /mnt/raw
  * projectaria-tools (aria_mps) is installed and on PATH
  * Ray cluster is running (`ray start --head ...` done in your YAML)
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


@ray.remote(num_cpus=1, memory=8 * 1024 ** 3)
def run_mps_on_folder(folder: str) -> dict:
    """Executes aria_mps for a given folder. Returns status dict."""
    try:
        ensure_token()
        cmd = ["aria_mps", "single", "-i", folder, "--no-ui", "--retry-failed"]
        subprocess.run(cmd, check=True)
        return {"folder": folder, "status": "ok"}
    except Exception as exc:
        return {
            "folder": folder,
            "status": "error",
            "error": f"{exc}",
            "trace": traceback.format_exc(limit=3),
        }


def discover_subfolders() -> list[str]:
    """Immediate sub-directories inside RAW_ROOT (skip files)."""
    if not RAW_ROOT.exists():
        raise RuntimeError(f"{RAW_ROOT} does not exist or is not mounted?")
    return [str(p) for p in RAW_ROOT.iterdir() if p.is_dir()]


def launch_jobs(folders: list[str]) -> None:
    """Fire Ray tasks & stream unordered results."""
    if not folders:
        print("Nothing to do – 0 sub-folders found.")
        return
    print(f"Launching {len(folders)} parallel MPS jobs…")
    # Start tasks
    futures = [run_mps_on_folder.remote(f) for f in folders]
    remaining = set(futures)
    while remaining:
        ready, remaining = ray.wait(list(remaining), num_returns=1, timeout=None)
        res = ray.get(ready[0])
        ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if res["status"] == "ok":
            print(f"[{ts}] ✓ {res['folder']}")
        else:
            print(f"[{ts}] ✗ {res['folder']} :: {res['error']}")
            print(res["trace"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run immediately instead of exiting if not between 00:00-02:00",
    )
    args = parser.parse_args()

    # In “cron mode” we only work between 00:00-02:00 local time.
    if not args.debug:
        hour = dt.datetime.now().hour
        if hour not in (0, 1):  # outside window → exit quietly
            sys.exit(0)

    # Connect to the local Ray cluster (running on the head node)
    ray.init(address="auto")

    folders = discover_subfolders()
    launch_jobs(folders)


if __name__ == "__main__":
    main()
