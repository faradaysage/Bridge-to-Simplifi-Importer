#!/usr/bin/env python3
"""bridge-to-simplifi.py

Bridge unsupported financial exports into Quicken Simplifi CSV imports.

This repo started as a Cash App → Simplifi bridge, but the architecture is intentionally
provider-agnostic so additional import adapters can be added cleanly.

Currently supported providers:
- Cash App (CSV export)
- Venmo (Account Statement CSV export)

Examples:
  python bridge-to-simplifi.py --cashapp --import cashapp.csv --export simplifi.csv
  python bridge-to-simplifi.py --venmo   --import venmo.csv   --export simplifi.csv

Rules + seen-rows conventions (defaults):
  rules.json            -> global rules (applies to all providers)
  rules-cashapp.json    -> Cash App-specific rules (optional)
  rules-venmo.json      -> Venmo-specific rules (optional)

  seen-rows-cashapp.txt -> Cash App dedupe store (optional)
  seen-rows-venmo.txt   -> Venmo dedupe store (optional)

If you pass --rules or --seen-rows explicitly, provider-specific variants are inferred unless
the filename already includes the provider. You can also use {provider} token:
  --seen-rows ./.state/seen-rows-{provider}.txt
  --rules     ./rules/{provider}.json
"""

from __future__ import annotations

import abc
import argparse
import csv
import json
import os
import re
import sys
import hashlib
from dataclasses import dataclass, replace
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Protocol, Sequence, Set, Tuple


# -----------------------------
# Core domain model
# -----------------------------

@dataclass(frozen=True)
class Transaction:
    """A generic transaction model used by the pipeline."""

    occurred_on: date
    payee: str
    amount: Decimal  # signed: negative outflow, positive inflow
    category: str = ""
    tags: Tuple[str, ...] = ()
    notes: str = ""
    check_no: str = ""

    def with_tags(self, tags: Sequence[str]) -> "Transaction":
        normalized = tuple(t.strip() for t in tags if t and t.strip())
        return replace(self, tags=normalized)


# -----------------------------
# URI abstraction
# -----------------------------

@dataclass(frozen=True)
class Uri:
    raw: str

    @property
    def scheme(self) -> str:
        s = self.raw.strip()
        if "://" in s:
            return s.split("://", 1)[0].lower()
        return "file"

    def as_path(self) -> Path:
        if self.scheme != "file":
            raise ValueError(f"Cannot convert non-file URI to path: {self.raw}")
        s = self.raw
        if s.lower().startswith("file://"):
            s = s[7:]
        return Path(s).expanduser().resolve()


# -----------------------------
# Notifier / output sinks
# -----------------------------

class LogLevel:
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


@dataclass(frozen=True)
class LogEvent:
    level: int
    message: str
    data: Optional[Dict[str, Any]] = None


class IMessageSink(Protocol):
    def emit(self, event: LogEvent) -> None: ...


class BufferedSink(IMessageSink):
    def __init__(self) -> None:
        self._events: List[LogEvent] = []

    def emit(self, event: LogEvent) -> None:
        self._events.append(event)

    def events(self) -> List[LogEvent]:
        return list(self._events)


class ConsoleSink(IMessageSink):
    _COLOR_RESET = "\x1b[0m"
    _COLOR_GRAY = "\x1b[90m"
    _COLOR_YELLOW = "\x1b[33m"
    _COLOR_RED = "\x1b[31m"
    _COLOR_MAGENTA = "\x1b[35m"

    def __init__(self, min_level: int = LogLevel.INFO, enable_color: bool = True) -> None:
        self._min_level = min_level
        self._enable_color = enable_color and self._stdout_supports_color()

    def emit(self, event: LogEvent) -> None:
        if event.level < self._min_level:
            return
        prefix = self._prefix(event.level)
        msg = event.message
        if event.data:
            msg += " " + self._format_kv(event.data)
        print(f"{prefix}{msg}", file=sys.stdout)

    def _format_kv(self, data: Dict[str, Any]) -> str:
        parts = [f"{k}={v}" for k, v in data.items()]
        return "(" + ", ".join(parts) + ")"

    def _stdout_supports_color(self) -> bool:
        if not sys.stdout.isatty():
            return False
        if os.name == "nt":
            return True
        return True

    def _prefix(self, level: int) -> str:
        label = {
            LogLevel.DEBUG: "[DEBUG] ",
            LogLevel.INFO: "[INFO] ",
            LogLevel.WARNING: "[WARN] ",
            LogLevel.ERROR: "[ERROR] ",
            LogLevel.CRITICAL: "[CRIT] ",
        }.get(level, "[LOG] ")

        if not self._enable_color:
            return label
        if level >= LogLevel.CRITICAL:
            return self._COLOR_MAGENTA + label + self._COLOR_RESET
        if level >= LogLevel.ERROR:
            return self._COLOR_RED + label + self._COLOR_RESET
        if level >= LogLevel.WARNING:
            return self._COLOR_YELLOW + label + self._COLOR_RESET
        if level <= LogLevel.DEBUG:
            return self._COLOR_GRAY + label + self._COLOR_RESET
        return label


class Notifier:
    def __init__(self, sinks: Sequence[IMessageSink]) -> None:
        self._sinks = list(sinks)

    def debug(self, message: str, **data: Any) -> None:
        self._emit(LogLevel.DEBUG, message, data or None)

    def info(self, message: str, **data: Any) -> None:
        self._emit(LogLevel.INFO, message, data or None)

    def warning(self, message: str, **data: Any) -> None:
        self._emit(LogLevel.WARNING, message, data or None)

    def error(self, message: str, **data: Any) -> None:
        self._emit(LogLevel.ERROR, message, data or None)

    def critical(self, message: str, **data: Any) -> None:
        self._emit(LogLevel.CRITICAL, message, data or None)

    def _emit(self, level: int, message: str, data: Optional[Dict[str, Any]]) -> None:
        event = LogEvent(level=level, message=message, data=data)
        for sink in self._sinks:
            sink.emit(event)


# -----------------------------
# Contexts
# -----------------------------

@dataclass(frozen=True)
class IContext:
    notifier: Notifier


@dataclass(frozen=True)
class ImportContext(IContext):
    source: Uri
    dedupe_store: Optional["IRowDedupeStore"] = None
    options: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class ExportContext(IContext):
    destination: Uri
    options: Optional[Dict[str, Any]] = None


# -----------------------------
# Dedupe store (seen rows)
# -----------------------------

class IRowDedupeStore(Protocol):
    def try_get_checksum(self, key: str) -> Optional[str]: ...
    def upsert(self, key: str, checksum: str) -> None: ...
    def flush(self) -> None: ...


class FileRowDedupeStore(IRowDedupeStore):
    """Stores seen rows in a strict text format.

    Supported formats (and only these):
    - ROW:<sha256>
    - <PREFIX>:<id><TAB><sha256>
    """

    def __init__(self, path: Path, notifier: Notifier) -> None:
        self._path = path
        self._notifier = notifier
        self._seen: Dict[str, str] = {}
        self._dirty = False
        self._load()

    def try_get_checksum(self, key: str) -> Optional[str]:
        if key not in self._seen:
            return None

        if key.startswith("ROW:"):
            embedded = key.split(":", 1)[1].strip()
            if not embedded:
                raise ValueError(f"Invalid seen-rows key (empty ROW checksum): {key}")
            return embedded

        checksum = self._seen.get(key, "").strip()
        if not checksum:
            raise ValueError(f"Invalid seen-rows entry (missing checksum for key): {key}")
        return checksum

    def upsert(self, key: str, checksum: str) -> None:
        checksum = (checksum or "").strip()
        if not checksum:
            raise ValueError(f"Cannot upsert empty checksum (key={key})")

        value = "" if key.startswith("ROW:") else checksum
        cur = self._seen.get(key, "")
        if cur != value:
            self._seen[key] = value
            self._dirty = True

    def flush(self) -> None:
        if not self._dirty:
            return

        self._path.parent.mkdir(parents=True, exist_ok=True)

        lines: List[str] = []
        for k in sorted(self._seen.keys()):
            if k.startswith("ROW:"):
                lines.append(k)
                continue

            v = (self._seen.get(k, "") or "").strip()
            if not v:
                raise ValueError(f"Invalid seen-rows entry (missing checksum for key): {k}")
            lines.append(f"{k}\t{v}")

        self._path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self._dirty = False
        self._notifier.debug("Seen-rows store flushed", path=str(self._path), keys=len(self._seen))

    def _load(self) -> None:
        if not self._path.exists():
            return

        text = self._path.read_text(encoding="utf-8")
        for line_num, line in enumerate(text.splitlines(), start=1):
            raw = line.strip()
            if not raw:
                continue

            if "\t" in raw:
                k, c = raw.split("\t", 1)
                k = k.strip()
                c = c.strip()

                if not k or not c:
                    raise ValueError(f"Invalid seen-rows line {line_num}: {raw}")
                if k.startswith("ROW:"):
                    raise ValueError(f"Invalid seen-rows line {line_num} (ROW cannot have checksum column): {raw}")

                self._seen[k] = c
                continue

            if not raw.startswith("ROW:"):
                raise ValueError(f"Invalid seen-rows line {line_num} (expected TAB or ROW:<sha256>): {raw}")

            embedded = raw.split(":", 1)[1].strip()
            if not embedded:
                raise ValueError(f"Invalid seen-rows line {line_num} (empty ROW checksum): {raw}")
            self._seen[raw] = ""

        self._notifier.debug("Seen-rows store loaded", path=str(self._path), keys=len(self._seen))


# -----------------------------
# Transformers (rules)
# -----------------------------

class ITransformer(Protocol):
    def transform(self, tx: Transaction) -> Optional[Transaction]: ...


@dataclass(frozen=True)
class MatchSpec:
    field: str
    type: str
    value: str


@dataclass(frozen=True)
class SetSpec:
    category: Optional[str] = None
    payee: Optional[str] = None
    tags: Optional[Tuple[str, ...]] = None


@dataclass(frozen=True)
class Rule:
    match: MatchSpec
    set: SetSpec


class RulesTransformer(ITransformer):
    """Applies first-match-wins rules loaded from JSON config."""

    def __init__(self, rules: Sequence[Rule], notifier: Notifier) -> None:
        self._rules = list(rules)
        self._notifier = notifier

    def transform(self, tx: Transaction) -> Optional[Transaction]:
        payee_l = tx.payee.lower()
        notes_l = tx.notes.lower()

        for rule in self._rules:
            if self._is_match(rule.match, payee_l, notes_l):
                new_tx = tx
                if rule.set.category is not None:
                    new_tx = replace(new_tx, category=rule.set.category)
                if rule.set.payee is not None:
                    new_tx = replace(new_tx, payee=rule.set.payee)
                if rule.set.tags is not None:
                    new_tx = new_tx.with_tags(rule.set.tags)
                return new_tx

        return tx

    def _is_match(self, spec: MatchSpec, payee_l: str, notes_l: str) -> bool:
        haystack = payee_l if spec.field.lower() == "payee" else notes_l
        needle = spec.value.lower()

        if spec.type.lower() == "contains":
            return needle in haystack
        if spec.type.lower() == "regex":
            try:
                return re.search(spec.value, haystack, flags=re.IGNORECASE) is not None
            except re.error:
                self._notifier.warning("Invalid regex in rules; ignoring", pattern=spec.value)
                return False

        self._notifier.warning("Unknown match type; ignoring", match_type=spec.type)
        return False

    @staticmethod
    def load_from_json(path: Path, notifier: Notifier) -> "RulesTransformer":
        if not path.exists():
            notifier.warning("Rules file not found; continuing without rules", path=str(path))
            return RulesTransformer([], notifier)

        data = json.loads(path.read_text(encoding="utf-8"))
        raw_rules = data.get("rules", [])
        rules: List[Rule] = []

        for i, rr in enumerate(raw_rules):
            try:
                m = rr["match"]
                s = rr.get("set", {})
                match = MatchSpec(
                    field=str(m.get("field", "")).strip(),
                    type=str(m.get("type", "")).strip(),
                    value=str(m.get("value", "")).strip(),
                )
                tags_raw = s.get("tags", None)
                tags: Optional[Tuple[str, ...]] = None
                if isinstance(tags_raw, list):
                    tags = tuple(str(t).strip() for t in tags_raw if str(t).strip())

                set_spec = SetSpec(
                    category=(str(s["category"]).strip() if "category" in s else None),
                    payee=(str(s["payee"]).strip() if "payee" in s else None),
                    tags=tags,
                )
                rules.append(Rule(match=match, set=set_spec))
            except Exception as ex:
                notifier.warning("Failed to parse rule; skipping", index=i, error=type(ex).__name__)

        notifier.info("Loaded rules", path=str(path), count=len(rules))
        return RulesTransformer(rules, notifier)


class PipelineTransformer(ITransformer):
    def __init__(self, transformers: Sequence[ITransformer]) -> None:
        self._transformers = list(transformers)

    def transform(self, tx: Transaction) -> Optional[Transaction]:
        cur: Optional[Transaction] = tx
        for t in self._transformers:
            if cur is None:
                return None
            cur = t.transform(cur)
        return cur


# -----------------------------
# Importers / Exporters
# -----------------------------

class IImporter(Protocol):
    def can_handle(self, source: Uri) -> bool: ...
    def import_transactions(self, context: ImportContext) -> Iterator[Transaction]: ...


class IExporter(Protocol):
    def can_handle(self, destination: Uri) -> bool: ...
    def export_transactions(self, context: ExportContext, transactions: Iterable[Transaction]) -> None: ...


class CsvImporterBase(abc.ABC):
    def can_handle(self, source: Uri) -> bool:
        if source.scheme != "file":
            return False
        p = source.as_path()
        # Provider selection is explicit via CLI, so we only require an existing file.
        # (Some providers export files without a strict .csv extension.)
        return p.exists() and p.is_file()

    @abc.abstractmethod
    def import_transactions(self, context: ImportContext) -> Iterator[Transaction]:
        raise NotImplementedError


class CsvExporterBase(abc.ABC):
    def can_handle(self, destination: Uri) -> bool:
        if destination.scheme != "file":
            return False
        p = destination.as_path()
        return p.suffix.lower() == ".csv" or not p.suffix

    @abc.abstractmethod
    def export_transactions(self, context: ExportContext, transactions: Iterable[Transaction]) -> None:
        raise NotImplementedError


class MoneyParser:
    @staticmethod
    def to_decimal(s: str) -> Decimal:
        """Parses strings like "$0.00", "-$45.00", "+ $8.41", "($45.00)", "-45.00"."""
        t = (s or "").strip().replace(",", "")
        t = t.replace("$", "")
        t = t.replace(" ", "")
        if t.startswith("+"):
            t = t[1:]
        if t.startswith("(") and t.endswith(")"):
            t = "-" + t[1:-1]
        try:
            return Decimal(t)
        except InvalidOperation as ex:
            raise ValueError(f"Invalid money value: {s}") from ex


# -----------------------------
# Cash App CSV Importer
# -----------------------------

class CashAppCsvImporter(CsvImporterBase):
    CA_DATE = "Date"
    CA_TXID = "Transaction ID"
    CA_TYPE = "Transaction Type"
    CA_AMOUNT = "Amount"
    CA_NET = "Net Amount"
    CA_STATUS = "Status"
    CA_NOTES = "Notes"
    CA_NAME = "Name of sender/receiver"
    CA_ACCOUNT = "Account"
    CA_ASSET_TYPE = "Asset Type"
    CA_ASSET_PRICE = "Asset Price"
    CA_ASSET_AMOUNT = "Asset Amount"

    DEFAULT_INCLUDE_TYPES = set()
    DEFAULT_REQUIRE_STATUS = {"COMPLETE"}

    def __init__(self, include_types: Optional[Set[str]] = None, require_status: Optional[Set[str]] = None) -> None:
        self._include_types = {t.upper() for t in (include_types or self.DEFAULT_INCLUDE_TYPES)}
        self._require_status = {s.upper() for s in (require_status or self.DEFAULT_REQUIRE_STATUS)}

    def import_transactions(self, context: ImportContext) -> Iterator[Transaction]:
        skipped: List[Dict[str, str]] = []
        skip_counts: Dict[str, int] = {}

        notifier = context.notifier
        path = context.source.as_path()

        notifier.info("Importing Cash App CSV", path=str(path))

        missing_txid_count = 0

        def record_skip(reason: str, row_num: int, row: Dict[str, str], key: str = "") -> None:
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            skipped.append(
                {
                    "Row": str(row_num),
                    "Reason": reason,
                    "Key": key,
                    self.CA_DATE: (row.get(self.CA_DATE) or "").strip(),
                    self.CA_TXID: (row.get(self.CA_TXID) or "").strip(),
                    self.CA_TYPE: (row.get(self.CA_TYPE) or "").strip(),
                    self.CA_STATUS: (row.get(self.CA_STATUS) or "").strip(),
                    self.CA_NET: (row.get(self.CA_NET) or "").strip(),
                    self.CA_AMOUNT: (row.get(self.CA_AMOUNT) or "").strip(),
                }
            )

        with path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=2):
                txid = (row.get(self.CA_TXID) or "").strip()
                status = (row.get(self.CA_STATUS) or "").strip().upper()
                ttype = (row.get(self.CA_TYPE) or "").strip().upper()

                if self._require_status and status and status not in self._require_status:
                    record_skip("status_filtered", row_num, row, key=(f"CASHAPP_TXID:{txid}" if txid else ""))
                    continue

                if self._include_types and ttype not in self._include_types:
                    record_skip("type_filtered", row_num, row, key=(f"CASHAPP_TXID:{txid}" if txid else ""))
                    continue

                checksum = self._row_checksum(row)
                dedupe_key = f"CASHAPP_TXID:{txid}" if txid else f"ROW:{checksum}"

                if not txid:
                    missing_txid_count += 1
                    notifier.debug("Row missing Transaction ID; using checksum key", row=row_num, key=dedupe_key)

                if context.dedupe_store:
                    prior = context.dedupe_store.try_get_checksum(dedupe_key)
                    if prior is not None:
                        if prior != checksum:
                            notifier.warning(
                                "Seen key has checksum mismatch",
                                row=row_num,
                                key=dedupe_key,
                                prior=prior,
                                current=checksum,
                                date=(row.get(self.CA_DATE) or "").strip(),
                                type=(row.get(self.CA_TYPE) or "").strip(),
                                amount=(row.get(self.CA_NET) or row.get(self.CA_AMOUNT) or "").strip(),
                            )
                        record_skip("duplicate_seen", row_num, row, key=dedupe_key)
                        continue

                try:
                    occurred_on = self._parse_cashapp_date(row.get(self.CA_DATE, ""))
                    payee = self._normalize_payee(row)
                    amount = self._choose_amount(row)
                    notes = self._build_notes(row)
                except Exception as ex:
                    record_skip(f"parse_error:{type(ex).__name__}", row_num, row, key=dedupe_key)
                    continue

                tx = Transaction(occurred_on=occurred_on, payee=payee, amount=amount, notes=notes)

                if context.dedupe_store:
                    context.dedupe_store.upsert(dedupe_key, checksum)

                yield tx

        if missing_txid_count:
            notifier.warning("Some rows had missing Transaction ID; checksums were used instead", count=missing_txid_count)

        skip_report = (context.options or {}).get("skip_report_path", "")
        if skipped and skip_report:
            with Path(skip_report).open("w", newline="", encoding="utf-8") as sf:
                writer = csv.DictWriter(
                    sf,
                    fieldnames=[
                        "Row",
                        "Reason",
                        "Key",
                        self.CA_DATE,
                        self.CA_TXID,
                        self.CA_TYPE,
                        self.CA_STATUS,
                        self.CA_NET,
                        self.CA_AMOUNT,
                    ],
                    quoting=csv.QUOTE_ALL,
                    lineterminator="\n",
                )
                writer.writeheader()
                for r in skipped:
                    writer.writerow(r)
            notifier.warning("Wrote skipped rows report", path=skip_report, rows=len(skipped))

        if skip_counts:
            notifier.warning("Skipped rows summary", **{f"{k}": v for k, v in sorted(skip_counts.items())})

        notifier.info("Import complete", path=str(path))

    def _row_checksum(self, row: Dict[str, str]) -> str:
        keys = sorted(row.keys())
        parts = []
        for k in keys:
            v = row.get(k, "")
            if v is None:
                v = ""
            parts.append(f"{k}={str(v).strip()}")
        blob = "\n".join(parts).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()

    def _parse_cashapp_date(self, value: str) -> date:
        s = (value or "").strip()
        if not s:
            raise ValueError("Missing Cash App Date")
        parts = s.split()
        dt = datetime.strptime(parts[0], "%Y-%m-%d")
        return dt.date()

    def _choose_amount(self, row: Dict[str, str]) -> Decimal:
        net = (row.get(self.CA_NET) or "").strip()
        amt = (row.get(self.CA_AMOUNT) or "").strip()
        if net:
            return MoneyParser.to_decimal(net)
        return MoneyParser.to_decimal(amt)

    def _normalize_payee(self, row: Dict[str, str]) -> str:
        name = (row.get(self.CA_NAME) or "").strip()
        if name:
            return name
        ttype = (row.get(self.CA_TYPE) or "").strip()
        return ttype or "Cash App"

    def _build_notes(self, row: Dict[str, str]) -> str:
        bits: List[str] = []

        txid = (row.get(self.CA_TXID) or "").strip()
        ttype = (row.get(self.CA_TYPE) or "").strip()
        status = (row.get(self.CA_STATUS) or "").strip()
        funding = (row.get(self.CA_ACCOUNT) or "").strip()
        user_notes = (row.get(self.CA_NOTES) or "").strip()

        if txid:
            bits.append(f"CashAppTxID={txid}")
        if ttype:
            bits.append(f"Type={ttype}")
        if status:
            bits.append(f"Status={status}")
        if funding:
            bits.append(f"Funding={funding}")
        if user_notes:
            bits.append(f"UserNotes={user_notes}")

        asset_type = (row.get(self.CA_ASSET_TYPE) or "").strip()
        asset_price = (row.get(self.CA_ASSET_PRICE) or "").strip()
        asset_amt = (row.get(self.CA_ASSET_AMOUNT) or "").strip()
        if asset_type or asset_price or asset_amt:
            bits.append(f"AssetType={asset_type or 'NA'}")
            if asset_price:
                bits.append(f"AssetPrice={asset_price}")
            if asset_amt:
                bits.append(f"AssetAmount={asset_amt}")

        return " | ".join(bits)


# -----------------------------
# Venmo CSV Importer
# -----------------------------

class VenmoCsvImporter(CsvImporterBase):
    """Venmo "Account Statement" CSV -> Transaction iterator.

    Venmo statement exports include preamble lines ("Account Statement", etc.).
    We scan for the real header row which begins with: ",ID,Datetime,Type,...".
    """

    V_ID = "ID"
    V_DATETIME = "Datetime"
    V_TYPE = "Type"
    V_STATUS = "Status"
    V_NOTE = "Note"
    V_FROM = "From"
    V_TO = "To"
    V_AMOUNT_TOTAL = "Amount (total)"
    V_FUNDING_SOURCE = "Funding Source"
    V_DESTINATION = "Destination"

    DEFAULT_INCLUDE_TYPES = set()
    DEFAULT_REQUIRE_STATUS = {"COMPLETE"}  # Venmo export uses "Complete" but we normalize to upper

    def __init__(self, include_types: Optional[Set[str]] = None, require_status: Optional[Set[str]] = None) -> None:
        self._include_types = {t.upper() for t in (include_types or self.DEFAULT_INCLUDE_TYPES)}
        self._require_status = {s.upper() for s in (require_status or self.DEFAULT_REQUIRE_STATUS)}

    def import_transactions(self, context: ImportContext) -> Iterator[Transaction]:
        skipped: List[Dict[str, str]] = []
        skip_counts: Dict[str, int] = {}

        notifier = context.notifier
        path = context.source.as_path()
        notifier.info("Importing Venmo CSV", path=str(path))

        def record_skip(reason: str, row_num: int, row: Dict[str, str], key: str = "") -> None:
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            skipped.append(
                {
                    "Row": str(row_num),
                    "Reason": reason,
                    "Key": key,
                    self.V_DATETIME: (row.get(self.V_DATETIME) or "").strip(),
                    self.V_ID: (row.get(self.V_ID) or "").strip(),
                    self.V_TYPE: (row.get(self.V_TYPE) or "").strip(),
                    self.V_STATUS: (row.get(self.V_STATUS) or "").strip(),
                    self.V_AMOUNT_TOTAL: (row.get(self.V_AMOUNT_TOTAL) or "").strip(),
                }
            )

        header_row_index, fieldnames = self._find_header(path)
        if not fieldnames:
            raise ValueError("Could not locate Venmo header row")

        with path.open("r", newline="", encoding="utf-8-sig") as f:
            # Skip preamble until header line
            for _ in range(header_row_index):
                f.readline()

            reader = csv.DictReader(f, fieldnames=fieldnames)
            # First row is the header itself; consume it
            next(reader, None)

            for physical_row_num, row in enumerate(reader, start=header_row_index + 2):
                # Venmo export's first column is blank; DictReader will include a "" key sometimes.
                row = {k.strip(): (v or "") for k, v in row.items() if k is not None}

                vid = (row.get(self.V_ID) or "").strip()
                status = (row.get(self.V_STATUS) or "").strip().upper()
                vtype = (row.get(self.V_TYPE) or "").strip().upper()

                if self._require_status and status and status not in self._require_status:
                    record_skip("status_filtered", physical_row_num, row, key=(f"VENMO_ID:{vid}" if vid else ""))
                    continue

                if self._include_types and vtype not in self._include_types:
                    record_skip("type_filtered", physical_row_num, row, key=(f"VENMO_ID:{vid}" if vid else ""))
                    continue

                checksum = self._row_checksum(row)
                dedupe_key = f"VENMO_ID:{vid}" if vid else f"ROW:{checksum}"

                if context.dedupe_store:
                    prior = context.dedupe_store.try_get_checksum(dedupe_key)
                    if prior is not None:
                        if prior != checksum:
                            notifier.warning(
                                "Seen key has checksum mismatch",
                                row=physical_row_num,
                                key=dedupe_key,
                                prior=prior,
                                current=checksum,
                                datetime=(row.get(self.V_DATETIME) or "").strip(),
                                type=(row.get(self.V_TYPE) or "").strip(),
                                amount=(row.get(self.V_AMOUNT_TOTAL) or "").strip(),
                            )
                        record_skip("duplicate_seen", physical_row_num, row, key=dedupe_key)
                        continue

                try:
                    occurred_on = self._parse_venmo_datetime(row.get(self.V_DATETIME, ""))
                    amount = MoneyParser.to_decimal(row.get(self.V_AMOUNT_TOTAL, ""))
                    payee = self._normalize_payee(row, amount)
                    notes = self._build_notes(row)
                except Exception as ex:
                    record_skip(f"parse_error:{type(ex).__name__}", physical_row_num, row, key=dedupe_key)
                    continue

                tx = Transaction(occurred_on=occurred_on, payee=payee, amount=amount, notes=notes)

                if context.dedupe_store:
                    context.dedupe_store.upsert(dedupe_key, checksum)

                yield tx

        skip_report = (context.options or {}).get("skip_report_path", "")
        if skipped and skip_report:
            with Path(skip_report).open("w", newline="", encoding="utf-8") as sf:
                writer = csv.DictWriter(
                    sf,
                    fieldnames=["Row", "Reason", "Key", self.V_DATETIME, self.V_ID, self.V_TYPE, self.V_STATUS, self.V_AMOUNT_TOTAL],
                    quoting=csv.QUOTE_ALL,
                    lineterminator="\n",
                )
                writer.writeheader()
                for r in skipped:
                    writer.writerow(r)
            notifier.warning("Wrote skipped rows report", path=skip_report, rows=len(skipped))

        if skip_counts:
            notifier.warning("Skipped rows summary", **{f"{k}": v for k, v in sorted(skip_counts.items())})

        notifier.info("Import complete", path=str(path))

    def _find_header(self, path: Path) -> Tuple[int, List[str]]:
        """Returns (0-based line index of header row, fieldnames)."""
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            for idx, line in enumerate(f):
                # The real header line begins with a leading comma (blank first column)
                # and contains the canonical set of fields.
                if line.startswith(",ID,") and ",Datetime," in line and ",Amount (total)," in line:
                    # Preserve the leading empty column as "" so DictReader alignment is correct.
                    raw_fields = [c.strip() for c in line.rstrip("\n").split(",")]
                    return idx, raw_fields
        return -1, []

    def _row_checksum(self, row: Dict[str, str]) -> str:
        keys = sorted(row.keys())
        parts = []
        for k in keys:
            v = row.get(k, "")
            if v is None:
                v = ""
            parts.append(f"{k}={str(v).strip()}")
        blob = "\n".join(parts).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()

    def _parse_venmo_datetime(self, value: str) -> date:
        s = (value or "").strip()
        if not s:
            raise ValueError("Missing Venmo Datetime")
        # Typical: 2025-07-05T05:51:55
        dt = datetime.fromisoformat(s)
        return dt.date()

    def _normalize_payee(self, row: Dict[str, str], amount: Decimal) -> str:
        from_ = (row.get(self.V_FROM) or "").strip()
        to = (row.get(self.V_TO) or "").strip()
        vtype = (row.get(self.V_TYPE) or "").strip()

        # Heuristic:
        # - If there's a To, treat that as the counterparty.
        # - Else if there's a From, treat that as the counterparty.
        # - Else fall back to Type.
        #
        # (We do NOT try to infer "me" from the statement handle; users can normalize via rules.)
        if to:
            return to
        if from_:
            return from_
        return vtype or "Venmo"

    def _build_notes(self, row: Dict[str, str]) -> str:
        bits: List[str] = []

        vid = (row.get(self.V_ID) or "").strip()
        vtype = (row.get(self.V_TYPE) or "").strip()
        status = (row.get(self.V_STATUS) or "").strip()
        note = (row.get(self.V_NOTE) or "").strip()
        from_ = (row.get(self.V_FROM) or "").strip()
        to = (row.get(self.V_TO) or "").strip()
        funding = (row.get(self.V_FUNDING_SOURCE) or "").strip()
        dest = (row.get(self.V_DESTINATION) or "").strip()

        if vid:
            bits.append(f"VenmoID={vid}")
        if vtype:
            bits.append(f"Type={vtype}")
        if status:
            bits.append(f"Status={status}")
        if from_:
            bits.append(f"From={from_}")
        if to:
            bits.append(f"To={to}")
        if funding:
            bits.append(f"Funding={funding}")
        if dest:
            bits.append(f"Destination={dest}")
        if note:
            bits.append(f"Note={note}")

        return " | ".join(bits)


# -----------------------------
# Simplifi CSV Exporter
# -----------------------------

class SimplifiCsvExporter(CsvExporterBase):
    HEADERS = ["Date", "Payee", "Amount", "Category", "Tags", "Notes", "Check_No"]

    def export_transactions(self, context: ExportContext, transactions: Iterable[Transaction]) -> None:
        notifier = context.notifier
        path = context.destination.as_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        notifier.info("Exporting Simplifi CSV", path=str(path))

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=self.HEADERS,
                quoting=csv.QUOTE_ALL,
                lineterminator="\n",
            )
            writer.writeheader()

            count = 0
            for tx in transactions:
                writer.writerow(self._to_row(tx))
                count += 1

        notifier.info("Export complete", path=str(path), rows=count)

    def _to_row(self, tx: Transaction) -> Dict[str, str]:
        d = tx.occurred_on
        date_str = f"{d.month}/{d.day}/{d.year}"
        tags_str = ", ".join(tx.tags) if tx.tags else ""
        amt_str = f"{tx.amount:.2f}"

        return {
            "Date": date_str,
            "Payee": tx.payee or "",
            "Amount": amt_str,
            "Category": tx.category or "",
            "Tags": tags_str,
            "Notes": tx.notes or "",
            "Check_No": tx.check_no or "",
        }


# -----------------------------
# Registries + orchestrator
# -----------------------------

class ImporterRegistry:
    def __init__(self, importers: Sequence[IImporter]) -> None:
        self._importers = list(importers)

    def resolve(self, source: Uri) -> IImporter:
        candidates = [i for i in self._importers if i.can_handle(source)]
        if not candidates:
            raise ValueError(f"No importer can handle source: {source.raw}")
        return candidates[0]


class ExporterRegistry:
    def __init__(self, exporters: Sequence[IExporter]) -> None:
        self._exporters = list(exporters)

    def resolve(self, destination: Uri) -> IExporter:
        candidates = [e for e in self._exporters if e.can_handle(destination)]
        if not candidates:
            raise ValueError(f"No exporter can handle destination: {destination.raw}")
        return candidates[0]


@dataclass(frozen=True)
class OrchestratorResult:
    imported: int
    exported: int
    dropped: int


class Orchestrator:
    def __init__(
        self,
        importer_registry: ImporterRegistry,
        exporter_registry: ExporterRegistry,
        transformer: Optional[ITransformer],
        notifier: Notifier,
    ) -> None:
        self._importers = importer_registry
        self._exporters = exporter_registry
        self._transformer = transformer
        self._notifier = notifier

    def run(self, import_context: ImportContext, export_context: ExportContext, dry_run: bool = False) -> OrchestratorResult:
        importer = self._importers.resolve(import_context.source)
        exporter = self._exporters.resolve(export_context.destination)

        self._notifier.info(
            "Resolved importer/exporter",
            importer=type(importer).__name__,
            exporter=type(exporter).__name__,
        )

        imported = 0
        exported = 0
        dropped = 0

        def pipeline() -> Iterator[Transaction]:
            nonlocal imported, dropped
            for tx in importer.import_transactions(import_context):
                imported += 1
                if self._transformer:
                    tx2 = self._transformer.transform(tx)
                    if tx2 is None:
                        dropped += 1
                        continue
                    yield tx2
                else:
                    yield tx

        if dry_run:
            for _ in pipeline():
                exported += 1
            self._notifier.warning("Dry run: no export written", would_export=exported)
        else:

            def counting_iter() -> Iterator[Transaction]:
                nonlocal exported
                for tx in pipeline():
                    exported += 1
                    yield tx

            exporter.export_transactions(export_context, counting_iter())

        if import_context.dedupe_store:
            import_context.dedupe_store.flush()

        self._notifier.info("Run summary", imported=imported, exported=exported, dropped=dropped)
        return OrchestratorResult(imported=imported, exported=exported, dropped=dropped)


# -----------------------------
# CLI
# -----------------------------

class Provider(str, Enum):
    CASHAPP = "cashapp"
    VENMO = "venmo"


def _parse_csv_set(raw: str) -> Set[str]:
    if not raw.strip():
        return set()
    return {x.strip().upper() for x in raw.split(",") if x.strip()}


def _insert_provider_suffix(path: Path, provider: Provider) -> Path:
    """Insert -{provider} before the final suffix.

    seen-rows.txt -> seen-rows-venmo.txt
    rules.json    -> rules-venmo.json
    """
    stem = path.stem
    if stem.lower().endswith(f"-{provider.value}"):
        return path
    return path.with_name(f"{stem}-{provider.value}{path.suffix}")


def _resolve_contextual_path(raw: str, provider: Provider, default_name: str) -> Path:
    """Resolves a potentially templated path, defaulting to ./<default_name>-{provider}.*"""
    if not raw.strip():
        base = Path(default_name).expanduser().resolve()
        return _insert_provider_suffix(base, provider)

    if "{provider}" in raw:
        rendered = raw.replace("{provider}", provider.value)
        return Path(rendered).expanduser().resolve()

    p = Path(raw).expanduser().resolve()
    # If user gave a provider-specific filename already, do nothing.
    if provider.value in p.name.lower():
        return p
    return _insert_provider_suffix(p, provider)


def _rules_paths(raw_rules: str, provider: Provider, notifier: Notifier) -> List[Path]:
    """Returns ordered list of rules files to load.

    - Always attempt global rules (rules.json by default)
    - Also attempt provider-specific rules (rules-<provider>.json)
    """
    paths: List[Path] = []

    if raw_rules.strip():
        base = Path(raw_rules).expanduser().resolve()
        paths.append(base)
        # Infer provider-specific variant unless already provider-specific.
        if provider.value not in base.name.lower():
            paths.append(_insert_provider_suffix(base, provider))
        return paths

    # Defaults: rules.json + rules-<provider>.json (both optional)
    base_default = Path("rules.json").resolve()
    paths.append(base_default)
    paths.append(_insert_provider_suffix(base_default, provider))

    # Noise control: only log missing defaults at debug level (load_from_json warns; we keep it).
    notifier.debug("Resolved rules paths", provider=provider.value, paths=[str(p) for p in paths])
    return paths


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Bridge financial exports into Simplifi import CSV")

    provider_group = ap.add_mutually_exclusive_group(required=False)
    provider_group.add_argument("--cashapp", action="store_true", help="Import a Cash App CSV export")
    provider_group.add_argument("--venmo", action="store_true", help="Import a Venmo Account Statement CSV export")

    # Canonical flags
    ap.add_argument("--import", dest="import_uri", required=True, help="Import URI/path (e.g. export.csv or file:///... )")
    ap.add_argument("--export", dest="export_uri", required=True, help="Export URI/path for Simplifi CSV (e.g. out.csv)")

    # Back-compat aliases (README previously used these)
    ap.add_argument("--input", dest="import_uri_alias", default="", help=argparse.SUPPRESS)
    ap.add_argument("--output", dest="export_uri_alias", default="", help=argparse.SUPPRESS)

    ap.add_argument(
        "--seen-rows",
        default="",
        help=(
            "Path to seen rows file (optional). If you pass a generic filename, a provider suffix is added. "
            "You can also use {provider} token."
        ),
    )
    ap.add_argument(
        "--rules",
        default="",
        help=(
            "Path to rules JSON (optional). If you pass a generic rules.json, a provider variant is also loaded if present."
        ),
    )

    ap.add_argument(
        "--include-types",
        default="",
        help="Comma-separated provider transaction Types to include (default: ALL)",
    )
    ap.add_argument(
        "--status",
        default="",
        help="Comma-separated provider Status values to include (default: COMPLETE)",
    )

    ap.add_argument("--dry-run", action="store_true", help="Import + transform, but do not write export file")
    ap.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = ap.parse_args(argv)

    # Back-compat alias mapping (only used if canonical flags omitted)
    import_uri_raw = args.import_uri or args.import_uri_alias
    export_uri_raw = args.export_uri or args.export_uri_alias

    buffered = BufferedSink()
    console = ConsoleSink(min_level=LogLevel.DEBUG if args.debug else LogLevel.INFO, enable_color=True)
    notifier = Notifier([buffered, console])
    
    def _detect_provider_from_csv(path: Path) -> Provider:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            for _ in range(25):
                line = f.readline()
                if not line:
                    break
                s = line.strip()

                if s.startswith(",ID,") and "Datetime" in s and "Amount (total)" in s:
                    return Provider.VENMO

                if "Transaction ID" in s and "Transaction Type" in s and "Amount" in s:
                    return Provider.CASHAPP

        raise ValueError("Unable to infer provider from CSV header")


    if args.cashapp:
        provider = Provider.CASHAPP
    elif args.venmo:
        provider = Provider.VENMO
    else:
        src_path = Uri(import_uri_raw).as_path()
        notifier.info("No provider flag supplied; attempting auto-detection", path=str(src_path))
        provider = _detect_provider_from_csv(src_path)
        notifier.info("Auto-detected provider", provider=provider.value)

    import_uri = Uri(import_uri_raw)
    export_uri = Uri(export_uri_raw)

    include_types = _parse_csv_set(args.include_types)
    statuses = _parse_csv_set(args.status)

    # Seen rows store is optional but strongly recommended.
    seen_rows_path = _resolve_contextual_path(args.seen_rows, provider, default_name="seen-rows.txt")
    dedupe_store: Optional[IRowDedupeStore] = None
    if args.seen_rows.strip() or seen_rows_path.exists():
        # If user explicitly set --seen-rows OR the default file already exists, use it.
        dedupe_store = FileRowDedupeStore(seen_rows_path, notifier)
        notifier.info("Using seen-rows store", provider=provider.value, path=str(seen_rows_path))
    else:
        notifier.info("Seen-rows store not configured (no dedupe)")

    export_path = export_uri.as_path()
    skip_report_path = export_path.with_name(export_path.name + f".skipped.{provider.value}.csv")

    import_context = ImportContext(
        notifier=notifier,
        source=import_uri,
        dedupe_store=dedupe_store,
        options={"skip_report_path": str(skip_report_path)},
    )
    export_context = ExportContext(notifier=notifier, destination=export_uri)

    # Build transformer pipeline
    transformers: List[ITransformer] = []
    for p in _rules_paths(args.rules, provider, notifier):
        # load_from_json is intentionally tolerant: it warns and returns empty rules if file missing
        if p.exists() or args.rules.strip() or p.name == "rules.json":
            transformers.append(RulesTransformer.load_from_json(p, notifier))

    # Filter out no-op rule transformers so logging isn't confusing.
    transformers = [t for t in transformers if isinstance(t, RulesTransformer) and getattr(t, "_rules", [])]  # type: ignore[attr-defined]

    pipeline_transformer: Optional[ITransformer] = PipelineTransformer(transformers) if transformers else None

    # Register importers/exporters based on provider.
    if provider == Provider.CASHAPP:
        importer_registry = ImporterRegistry([CashAppCsvImporter(include_types=include_types, require_status=(statuses or None))])
    else:
        importer_registry = ImporterRegistry([VenmoCsvImporter(include_types=include_types, require_status=(statuses or None))])

    exporter_registry = ExporterRegistry([SimplifiCsvExporter()])

    orchestrator = Orchestrator(
        importer_registry=importer_registry,
        exporter_registry=exporter_registry,
        transformer=pipeline_transformer,
        notifier=notifier,
    )

    try:
        orchestrator.run(import_context, export_context, dry_run=args.dry_run)
        return 0
    except Exception as ex:
        notifier.critical("Fatal error", error=type(ex).__name__, detail=str(ex))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
