# CashApp → Simplifi Import Bridge

A robust, extensible bridge for converting Cash App exports into **Quicken Simplifi** CSV imports.

This project is intentionally architected as more than a one-off script. It is designed as a foundation for:
- Supporting additional financial institutions
- Supporting API-based bridges in the future
- Adding transformation pipelines (payees, categories, tags, metadata)
- Eventually evolving into a hosted or service-backed integration

---

## Features

- Streams CSV → Simplifi without loading entire datasets into memory
- Deduplicates previously imported rows using a persistent **seen-rows database**
- Supports **row checksum fallback** when Transaction IDs are missing
- Rule-based transformations (categories, payees, tags, notes)
- Generates **skipped-row forensic reports** so input/output mismatches are explainable
- Designed with importer/exporter interfaces so new sources can be added cleanly

---

## Prerequisites

### Required
- Python **3.10+**
- Standard library only (no third‑party packages required)

### Recommended
- Git (to clone and update the repo)
- VS Code or another Python‑friendly editor

### Verify Python
```bash
python --version
```

---

## Basic Usage

```bash
python cashapp-to-simplifi.py \
  --input cashapp-export.csv \
  --output simplifi-import.csv
```

---

## Optional Flags

| Flag | Purpose |
|------|--------|
| `--seen-rows seen.json` | Enables deduplication across runs |
| `--rules rules.json` | Applies transformation rules |
| `--debug` | Emits verbose diagnostics |
| `--dry-run` | Runs pipeline without writing output |
| `--skip-report skipped.csv` | Writes skipped-row audit report |

---

## How Deduplication Works (`--seen-rows`)

The script maintains a persistent store of rows that have already been exported.

### Identity Strategy
1. **If Transaction ID exists** → use it as the dedupe key  
2. **If Transaction ID is missing** → compute a deterministic checksum from the row  
3. If a row reappears with the same checksum → skipped  
4. If a key reappears but checksum mismatches → warning surfaced  

This allows:
- Safe re-runs
- Partial imports
- Recovery from mistakes

---

## Repairing a Partial Import (Common Scenario)

### Scenario
You already imported **some** Cash App transactions into Simplifi  
Now you want to import the **rest**, without duplicating prior entries.

### Recommended Repair Flow

#### Step 1 — Build an initial seen-rows database
Edit your Cash App export to **include transactions already present in Simplifi**

```bash
python cashapp-to-simplifi.py \
  --input cashapp-export-with-old.csv \
  --seen-rows seen.json \
  --dry-run
```

This seeds the dedupe database without exporting anything.

#### Step 2 — Run the real import on the full export
```bash
python cashapp-to-simplifi.py \
  --input cashapp-export-full.csv \
  --seen-rows seen.json \
  --output simplifi-import.csv
```

Result:
- Previously imported transactions are skipped
- Only **new transactions** appear in the output

---

## Rules System Overview (`--rules rules.json`)

Rules allow transformation **without modifying the script**.

### Supported Transformations
- Rename payees
- Override categories
- Add or strip tags
- Modify notes
- Conditional matching on:
  - Payee
  - Notes
  - Amount
  - Transaction type

---

## Example Rules File

```json
{
  "rules": [
    {
      "match": {
        "payee_contains": "STARBUCKS"
      },
      "set": {
        "category": "Coffee",
        "tags": ["food", "treat"]
      }
    },
    {
      "match": {
        "notes_contains": "VENMO"
      },
      "set": {
        "payee": "Venmo Transfer"
      }
    }
  ]
}
```

---

## Partial Overrides (Let Other Columns Pass Through)

Rules **only override fields explicitly declared**.

Example:
```json
"set": {
  "category": "Dining"
}
```

This modifies **only** the category  
All other fields pass through unchanged

This allows:
- Incremental rule building
- Minimal mutation
- Non‑destructive transforms

---

## Skipped Rows & Audit Trail

The script **always tracks skipped rows**, and can optionally write them:

```bash
--skip-report skipped.csv
```

Skipped rows include:
- Original row number
- Skip reason
- Transaction ID (if present)
- Amount, type, date, status
- Deduplication key

This ensures **no silent data loss**

---

## Common Scenarios

### Import New Transactions Weekly
```bash
python cashapp-to-simplifi.py \
  --input cashapp-latest.csv \
  --seen-rows seen.json \
  --output simplifi.csv
```

### Dry Run Before Real Import
```bash
python cashapp-to-simplifi.py \
  --input cashapp.csv \
  --seen-rows seen.json \
  --dry-run
```

### Debug Skipped Rows
```bash
python cashapp-to-simplifi.py \
  --input cashapp.csv \
  --debug \
  --skip-report skipped.csv
```

---

## Architectural Direction

This repo is intentionally built to evolve into:

- Multi‑bank import adapters
- API bridges instead of CSV
- Server‑based ingestion pipelines
- Eventually a hosted paid integration layer

The current Python design mirrors **C#‑friendly OOP patterns**, so a future port remains clean.

---

## Philosophy

This tool prioritizes:
- Determinism
- Explainability
- Idempotence
- Extensibility
- Auditability

Financial imports should never feel like guesswork.

---

## License

MIT — use freely, fork freely, build responsibly
