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
import shutil

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

@ray.remote(num_cpus=4, memory=16 * 1024 ** 3)
def convert_vrs_to_mp4(vrs_path: str) -> dict:
    try:
        mp4_path = Path(vrs_path).with_suffix(".mp4")
        if mp4_path.exists():
            return {"vrs": vrs_path, "status": "skipped"}

        tmp_output = Path("/tmp") / mp4_path.name
        print(f"[convert] Running vrs_to_mp4 on {vrs_path} → temp: {tmp_output}")

        result = subprocess.run(
            ["vrs_to_mp4", "--vrs", str(vrs_path), "--output_video", str(tmp_output)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        if result.returncode != 0:
            print(f"[convert] ❌ vrs_to_mp4 failed for {vrs_path}")
            print(f"[convert] --- STDOUT ---\n{result.stdout}")
            print(f"[convert] --- STDERR ---\n{result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, result.args, output=result.stdout, stderr=result.stderr)

        print(f"[convert] Moving {tmp_output} → {mp4_path}")
        shutil.move(str(tmp_output), str(mp4_path))

        if mp4_path.exists():
            print(f"[convert] ✅ Move succeeded: {mp4_path}")
        else:
            print(f"[convert] ❌ Move failed: {mp4_path} not found after move")

        return {"vrs": vrs_path, "status": "ok"}

    except Exception as exc:
        return {
            "vrs": vrs_path,
            "status": "err",
            "err": str(exc),
            "trace": traceback.format_exc(limit=2),
        }
        
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

    if args.debug:  # ===== DRY-RUN =====
        print("DEBUG MODE – nothing will run")
        for f in folders:
            print(" would process:", f)
        sys.exit(0)

    # ===== RAY INIT =====
    ray.init(address="auto")

    # ===== VRS TO MP4 CONVERSIONS =====
    vrs_files = []
    for folder in folders:
        vrs_files.extend(Path(folder).glob("*.vrs"))

    print(f"Discovered {len(vrs_files)} .vrs files, dispatching conversion tasks...")
    futures = [convert_vrs_to_mp4.remote(str(vrs)) for vrs in vrs_files]
    pending = set(futures)
    while pending:
        ready, pending = ray.wait(list(pending), num_returns=1)
        res = ray.get(ready[0])
        ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if res["status"] == "ok":
            print(f"[{ts}] ✓ Converted {res['vrs']}")
        elif res["status"] == "skipped":
            print(f"[{ts}] ↷ Skipped (already exists) {res['vrs']}")
        else:
            print(f"[{ts}] ✗ Failed to convert {res['vrs']} :: {res['err']}")
            print(res["trace"])

    # ===== MPS JOBS =====
    launch_jobs(folders)

if __name__ == "__main__":
    main()
