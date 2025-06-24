#!/usr/bin/env python3
"""
Run Aria MPS (single-folder mode) once per task directory every time the script
is invoked.  Designed to be launched nightly by cron/systemd on the Ray head
node.

Usage
-----
python run_daily_mps.py            # Scan /mnt/raw/<task>     and process each one
python run_daily_mps.py --root /mnt/raw --dry-run     # Just print what it would run
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

DEFAULT_RAW_ROOT = Path("/mnt/raw")         # where the S3 mount lives
TOKEN_VALUE = (
    "FRLAbl7Eqw5g4upZCoow1ht3YE9e16ue9iLTv13IpnXXxxt8gR5BXrqkuj6deunEcnDAUMNjylAZ"
    "AvSKzZB6PB2amlGFec8dyuvpaZAtZB0hxxzRRHgoy9gdZChM8lUDalGDP1q8VPoszMZBLiYoif0Q"
    "9aL49Ewn0mXUEVd3gHBW74yAwZD"
)
TOKEN_PATH = Path.home() / ".projectaria" / "auth_token"

# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

def ensure_aria_token():
    """Make sure ~/.projectaria/auth_token exists (for aria_mps)."""
    if TOKEN_PATH.exists():
        return
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_PATH.write_text(TOKEN_VALUE)
    print(f"[INFO] Wrote token to {TOKEN_PATH}", file=sys.stderr)


def run_mps(task_folder: Path, dry_run: bool = False) -> bool:
    """
    Execute `aria_mps single -i <task_folder>`.

    Returns True on success, False otherwise.
    """
    cmd = [
        "aria_mps",
        "single",
        "-i",
        str(task_folder),
        "--no-ui",
        "--retry-failed",
    ]
    if dry_run:
        print("[DRY-RUN]", " ".join(cmd))
        return True

    print(f"[{datetime.utcnow().isoformat()}] Running MPS on {task_folder}")
    try:
        subprocess.run(cmd, check=True)
        print(f"[OK]   {task_folder}")
        return True
    except subprocess.CalledProcessError as exc:
        print(f"[ERR]  {task_folder}: {exc}", file=sys.stderr)
        return False


def discover_task_folders(raw_root: Path):
    """
    Yield every immediate sub-directory of `raw_root`
    that contains at least one *.vrs file.
    """
    for child in sorted(raw_root.iterdir()):
        if child.is_dir() and any(child.glob("*.vrs")):
            yield child


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_RAW_ROOT,
        help="Top-level directory that contains task subfolders (default: /mnt/raw)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print what would run")
    args = parser.parse_args()

    if not args.root.is_dir():
        print(f"[FATAL] Raw root {args.root} does not exist or is not a directory",
              file=sys.stderr)
        sys.exit(1)

    ensure_aria_token()

    any_failed = False
    for task_dir in discover_task_folders(args.root):
        if not run_mps(task_dir, dry_run=args.dry_run):
            any_failed = True

    sys.exit(1 if any_failed else 0)


if __name__ == "__main__":
    main()
