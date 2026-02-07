#!/usr/bin/env python3

import csv
import sys
from pathlib import Path

def fail(msg: str):
    print(f"[FAIL] {msg}", file=sys.stderr)
    sys.exit(1)

def main():
    if len(sys.argv) < 3:
        fail("Usage: csv-merge.py <output.csv> <input1.csv> [input2.csv ...]")

    out_path = Path(sys.argv[1])
    in_paths = [Path(p) for p in sys.argv[2:]]

    header = None
    rows_written = 0

    with out_path.open("w", newline="", encoding="utf-8") as out_f:
        writer = None

        for p in in_paths:
            if not p.exists():
                fail(f"Input file not found: {p}")

            with p.open("r", newline="", encoding="utf-8") as in_f:
                reader = csv.reader(in_f)
                try:
                    file_header = next(reader)
                except StopIteration:
                    continue  # empty file

                if header is None:
                    header = file_header
                    writer = csv.writer(out_f)
                    writer.writerow(header)
                elif file_header != header:
                    fail(f"Header mismatch in {p}")

                for row in reader:
                    writer.writerow(row)
                    rows_written += 1

    print(f"[OK] Merged {len(in_paths)} files, wrote {rows_written} rows -> {out_path}")

if __name__ == "__main__":
    main()
