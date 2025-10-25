#!/usr/bin/env python3
"""
run_daily_mps.py – ALWAYS processes every /mnt/raw/<folder>
  • normal call: launches Ray jobs immediately
  • --debug: dry-run (lists the folders, no work)
  • Single-node Ray: spins up one large local Ray node
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

MPS_INI_CONTENT = """[DEFAULT]
log_dir = /tmp/logs/projectaria/mps/ # Path to log directory
status_check_interval = 30 # Status check interval in seconds

[HASH]
concurrent_hashes = 16 # Maximum number of recordings whose hashes will be calculated concurrently
chunk_size = 10485760 # 10 * 2**20 (10MB)

[HEALTH_CHECK]
concurrent_health_checks = 2  # Maximum number of checks that can run concurrently

[ENCRYPTION]
chunk_size = 52428800 # 50 * 2**20 (50MB)
concurrent_encryptions = 5 # Maximum number of recordings that will be encrypted concurrently
delete_encrypted_files = true # Delete encrypted files after upload is done

[UPLOAD]
backoff = 1.5 # Backoff factor for retries
concurrent_uploads = 16 # Maximum number of concurrent uploads
interval = 20 # Interval between runs
max_chunk_size = 104857600 # 100 * 2**20 (100 MB)
min_chunk_size = 5242880 # 5 * 2**20 (5MB)
retries = 10 # Number of times to retry a failed upload
smoothing_window_size = 10 # Size of the smoothing window
target_chunk_upload_secs = 3 # Target duration to upload a chunk

[DOWNLOAD]
backoff = 1.5 # Backoff factor for retries
chunk_size = 10485760 # 10 * 2**20 (10MB)
concurrent_downloads = 10 # Maximum number of concurrent downloads
delete_zip = true # Delete zip files after extracting
interval = 20 # Interval between runs
retries = 10 # Number of times to retry a failed upload

[GRAPHQL]
backoff = 1.5 # Backoff factor for retries
interval = 4 # Interval between runs
retries = 3 # Number of times to retry a failed upload
"""


def ensure_token_and_config():
    """
    Ensures both the ProjectAria token and mps.ini configuration exist
    on the local node (each Ray worker runs this).
    """
    projectaria_dir = Path.home() / ".projectaria"
    projectaria_dir.mkdir(parents=True, exist_ok=True)

    token_path = projectaria_dir / "auth_token"
    if not token_path.exists():
        token_path.write_text(TOKEN)

    ini_path = projectaria_dir / "mps.ini"
    ini_path.write_text(MPS_INI_CONTENT)


@ray.remote
def run_mps_on_folder(folder: str) -> dict:
    try:
        ensure_token_and_config()
        subprocess.run(
            ["aria_mps", "single", "-i", folder, "--no-ui", "--retry-failed"],
            check=True,
        )
        return {"folder": folder, "status": "ok"}
    except Exception as exc:
        return {
            "folder": folder,
            "status": "err",
            "err": str(exc),
            "trace": traceback.format_exc(limit=3),
        }


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

    if args.debug:
        print("DEBUG MODE – nothing will run")
        for f in folders:
            print(" would process:", f)
        sys.exit(0)

    # Initialize Ray on single local node
    ray.init()

    # Ensure config on head node too
    ensure_token_and_config()

    # Launch MPS jobs
    launch_jobs(folders)


if __name__ == "__main__":
    main()
