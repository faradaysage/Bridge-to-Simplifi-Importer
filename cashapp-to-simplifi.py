#!/usr/bin/env python3
"""cashapp-to-simplifi.py

DEPRECATED ENTRYPOINT.

This repo has grown beyond Cash App-only support. The canonical entrypoint is now:
  bridge-to-simplifi.py

This wrapper is kept for backwards compatibility, but you MUST now specify a provider flag
when invoking the underlying script.

Examples:
  python cashapp-to-simplifi.py --cashapp --import cashapp.csv --export simplifi.csv
  python cashapp-to-simplifi.py --venmo   --import venmo.csv   --export simplifi.csv
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


def main() -> int:
    here = Path(__file__).resolve().parent
    target = here / "bridge-to-simplifi.py"
    if not target.exists():
        print("[ERROR] bridge-to-simplifi.py not found next to this wrapper.")
        return 2

    # Execute the real script in-process, preserving argv.
    runpy.run_path(str(target), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
