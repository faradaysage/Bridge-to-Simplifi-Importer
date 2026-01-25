#!/usr/bin/env python3
"""
bridge_to_simplifi.py

A foundation for bridging unsupported financial institutions into Quicken Simplifi imports.

Design goals:
- Importer/exporter discovery via Strategy/Factory (CanHandle(uri))
- Orchestrator coordinates import -> transform pipeline -> export
- Strong seams: URI abstraction, contexts, transformers, notifier/output sink, dedupe store
- Robust CSV read/write using Python csv module with QUOTE_ALL to match Simplifi template style
- Single-file today; classes are encapsulated so they can be split into modules later with minimal changes

Current included adapters:
- Cash App CSV Importer (file://... or plain path)
- Simplifi CSV Exporter (file://... or plain path)

Example usage:
  python bridge_to_simplifi.py \
    --import cashapp.csv \
    --export simplifi_import.csv \
    --seen-rows seen_rows.txt \
    --rules rules.json \
    --include-types P2P \
    --status COMPLETE

Notes:
- This does NOT call Simplifi APIs (unknown/unsupported). It produces a Simplifi import CSV.
- Cash App "Account" column is a funding source, not a Simplifi account. We preserve it in Notes.

rules.json example (optional):
{
  "rules": [
    {
      "match": { "field": "payee", "type": "contains", "value": "joe" },
      "set":   { "category": "Lawn Care", "payee": "Joe's Lawn", "tags": ["Lawn"] }
    },
    {
      "match": { "field": "notes", "type": "contains", "value": "food" },
      "set":   { "category": "Restaurants", "payee": "", "tags": ["Food"] }
    },
    {
      "match": { "field": "payee", "type": "regex", "value": ".*uber.*" },
      "set":   { "category": "Rideshare", "tags": ["Travel"] }
    }
  ]
}

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
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Protocol, Sequence, Set, Tuple, Union


# -----------------------------
# Core domain model
# -----------------------------

@dataclass(frozen=True)
class Transaction:
    """
    A generic, in-app transaction model (the 'universal' representation).

    Exporters transform this model into their required wire format (Simplifi CSV today).
    Importers transform source-specific rows/events into this model.
    """
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
        # naive scheme detection; extensible later
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
    """
    Buffers events for the duration of a run. A downstream sink can flush at end.
    Useful for future email/SMS 'one message per run' strategies.
    """
    def __init__(self) -> None:
        self._events: List[LogEvent] = []

    def emit(self, event: LogEvent) -> None:
        self._events.append(event)

    def events(self) -> List[LogEvent]:
        return list(self._events)


class ConsoleSink(IMessageSink):
    """
    Console output with simple ANSI colors. If ANSI is not supported, it degrades gracefully.
    """
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
            # keep it compact; structured sinks can be added later
            msg += " " + self._format_kv(event.data)
        print(f"{prefix}{msg}", file=sys.stdout)

    def _format_kv(self, data: Dict[str, Any]) -> str:
        parts = []
        for k, v in data.items():
            parts.append(f"{k}={v}")
        return "(" + ", ".join(parts) + ")"

    def _stdout_supports_color(self) -> bool:
        if not sys.stdout.isatty():
            return False
        if os.name == "nt":
            # Modern Windows terminals usually support ANSI; if not, it'll just show raw codes.
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
    """
    Minimal facade so the rest of the system never calls print() directly.
    Swap sinks later (email, SMS, webhook).
    """
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
# Contexts (import/export)
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
    """
    Stores seen rows in a strict text format.

    Supported formats (and only these):
    - ROW:<sha256>                (no tab; checksum is embedded in the key)
    - TXID:<id><TAB><sha256>      (tab-separated; checksum stored as value)

    Any other line format is invalid and should fail.
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

        # ROW:<checksum> embeds the checksum in the key.
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

        # ROW keys embed the checksum, so the stored value is intentionally blank.
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
                # Strict: ROW format is key-only.
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

            # No-tab lines are only allowed for ROW:<sha256>
            if not raw.startswith("ROW:"):
                raise ValueError(f"Invalid seen-rows line {line_num} (expected TAB or ROW:<sha256>): {raw}")

            embedded = raw.split(":", 1)[1].strip()
            if not embedded:
                raise ValueError(f"Invalid seen-rows line {line_num} (empty ROW checksum): {raw}")

            self._seen[raw] = ""

        self._notifier.debug("Seen-rows store loaded", path=str(self._path), keys=len(self._seen))


# -----------------------------
# Transformers (configurable seams)
# -----------------------------

class ITransformer(Protocol):
    def transform(self, tx: Transaction) -> Optional[Transaction]:
        """
        Return transformed transaction, or None to drop it.
        """
        ...


@dataclass(frozen=True)
class MatchSpec:
    field: str  # "payee" or "notes" (extensible)
    type: str   # "contains" or "regex"
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
    """
    Applies first-match-wins rules loaded from external JSON config.
    """
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
    """
    Composes transformers. Any transformer can drop a tx by returning None.
    """
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
# Importers / Exporters (interfaces + base classes)
# -----------------------------

class IImporter(Protocol):
    def can_handle(self, source: Uri) -> bool: ...
    def import_transactions(self, context: ImportContext) -> Iterator[Transaction]: ...


class IExporter(Protocol):
    def can_handle(self, destination: Uri) -> bool: ...
    def export_transactions(self, context: ExportContext, transactions: Iterable[Transaction]) -> None: ...


class CsvImporterBase(abc.ABC):
    """
    Base class for CSV importers.
    """
    def can_handle(self, source: Uri) -> bool:
        if source.scheme != "file":
            return False
        p = source.as_path()
        return p.suffix.lower() == ".csv" and p.exists()

    @abc.abstractmethod
    def import_transactions(self, context: ImportContext) -> Iterator[Transaction]:
        raise NotImplementedError


class CsvExporterBase(abc.ABC):
    """
    Base class for CSV exporters.
    """
    def can_handle(self, destination: Uri) -> bool:
        if destination.scheme != "file":
            return False
        p = destination.as_path()
        return p.suffix.lower() == ".csv" or not p.suffix  # allow writing to path without suffix

    @abc.abstractmethod
    def export_transactions(self, context: ExportContext, transactions: Iterable[Transaction]) -> None:
        raise NotImplementedError


# -----------------------------
# Cash App CSV Importer
# -----------------------------

class CashAppCsvImporter(CsvImporterBase):
    """
    Cash App transaction CSV -> Transaction iterator.
    """

    # Cash App headers (from the export template)
    CA_DATE = "Date"
    CA_TXID = "Transaction ID"
    CA_TYPE = "Transaction Type"
    CA_AMOUNT = "Amount"
    CA_FEE = "Fee"
    CA_NET = "Net Amount"
    CA_STATUS = "Status"
    CA_NOTES = "Notes"
    CA_NAME = "Name of sender/receiver"
    CA_ACCOUNT = "Account"
    CA_ASSET_TYPE = "Asset Type"
    CA_ASSET_PRICE = "Asset Price"
    CA_ASSET_AMOUNT = "Asset Amount"

    DEFAULT_INCLUDE_TYPES = set()  # empty = allow all
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
            skipped.append({
                "Row": str(row_num),
                "Reason": reason,
                "Key": key,
                self.CA_DATE: (row.get(self.CA_DATE) or "").strip(),
                self.CA_TXID: (row.get(self.CA_TXID) or "").strip(),
                self.CA_TYPE: (row.get(self.CA_TYPE) or "").strip(),
                self.CA_STATUS: (row.get(self.CA_STATUS) or "").strip(),
                self.CA_NET: (row.get(self.CA_NET) or "").strip(),
                self.CA_AMOUNT: (row.get(self.CA_AMOUNT) or "").strip(),
            })

        with path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=2):
                txid = (row.get(self.CA_TXID) or "").strip()

                # status/type filters
                status = (row.get(self.CA_STATUS) or "").strip().upper()
                ttype = (row.get(self.CA_TYPE) or "").strip().upper()

                if self._require_status and status and status not in self._require_status:
                    record_skip("status_filtered", row_num, row, key=(f"TXID:{txid}" if txid else ""))
                    continue
                
                if self._include_types and ttype not in self._include_types:
                    record_skip("type_filtered", row_num, row, key=(f"TXID:{txid}" if txid else ""))
                    continue

                checksum = self._row_checksum(row)
                dedupe_key = f"TXID:{txid}" if txid else f"ROW:{checksum}"

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

                tx = Transaction(
                    occurred_on=occurred_on,
                    payee=payee,
                    amount=amount,
                    notes=notes,
                )

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
                    fieldnames=["Row", "Reason", "Key", self.CA_DATE, self.CA_TXID, self.CA_TYPE, self.CA_STATUS, self.CA_NET, self.CA_AMOUNT],
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
        """
        Stable checksum for a CSV row.
        Uses a deterministic key order and normalized string values.
        """
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
        """
        Cash App format often: 2026-01-08 11:55:33 EST
        We only need the date portion for Simplifi CSV import.
        """
        s = (value or "").strip()
        if not s:
            # fall back to 'today' is dangerous; raise instead
            raise ValueError("Missing Cash App Date")
        parts = s.split()
        # [YYYY-MM-DD, HH:MM:SS, TZ] or [YYYY-MM-DD, HH:MM:SS] or [YYYY-MM-DD]
        dt = datetime.strptime(parts[0], "%Y-%m-%d")
        return dt.date()

    def _money_to_decimal(self, s: str) -> Decimal:
        """
        Parses strings like "$0.00", "-$45.00", "($45.00)", "-45.00"
        """
        t = (s or "").strip().replace(",", "")
        t = t.replace("$", "")
        if t.startswith("(") and t.endswith(")"):
            t = "-" + t[1:-1]
        t = t.replace(" ", "")
        try:
            return Decimal(t)
        except InvalidOperation as ex:
            raise ValueError(f"Invalid money value: {s}") from ex

    def _choose_amount(self, row: Dict[str, str]) -> Decimal:
        net = (row.get(self.CA_NET) or "").strip()
        amt = (row.get(self.CA_AMOUNT) or "").strip()
        if net:
            return self._money_to_decimal(net)
        return self._money_to_decimal(amt)

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
# Simplifi CSV Exporter
# -----------------------------

class SimplifiCsvExporter(CsvExporterBase):
    """
    Transaction iterator -> Simplifi import CSV.

    Columns (matches official template):
      Date, Payee, Amount, Category, Tags, Notes, Check_No

    We write QUOTE_ALL to match the template style consistently.
    """
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
        # Simplifi template uses M/D/YYYY
        d = tx.occurred_on
        date_str = f"{d.month}/{d.day}/{d.year}"

        tags_str = ", ".join(tx.tags) if tx.tags else ""
        amt_str = f"{tx.amount:.2f}"  # Decimal -> 2dp

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
# Registries + factories (Strategy/CanHandle)
# -----------------------------

class ImporterRegistry:
    def __init__(self, importers: Sequence[IImporter]) -> None:
        self._importers = list(importers)

    def resolve(self, source: Uri) -> IImporter:
        candidates = [i for i in self._importers if i.can_handle(source)]
        if not candidates:
            raise ValueError(f"No importer can handle source: {source.raw}")
        # If multiple importers match, a future scoring system can pick the best.
        return candidates[0]


class ExporterRegistry:
    def __init__(self, exporters: Sequence[IExporter]) -> None:
        self._exporters = list(exporters)

    def resolve(self, destination: Uri) -> IExporter:
        candidates = [e for e in self._exporters if e.can_handle(destination)]
        if not candidates:
            raise ValueError(f"No exporter can handle destination: {destination.raw}")
        return candidates[0]


# -----------------------------
# Orchestrator
# -----------------------------

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

        self._notifier.info("Resolved importer/exporter", importer=type(importer).__name__, exporter=type(exporter).__name__)

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
            # Consume pipeline to count what would be exported.
            for _ in pipeline():
                exported += 1
            self._notifier.warning("Dry run: no export written", would_export=exported)
        else:
            # Exporter consumes the pipeline (streaming friendly).
            # If later we need atomic exports, the orchestrator can buffer.
            def counting_iter() -> Iterator[Transaction]:
                nonlocal exported
                for tx in pipeline():
                    exported += 1
                    yield tx

            exporter.export_transactions(export_context, counting_iter())

        # flush dedupe store if present
        if import_context.dedupe_store:
            import_context.dedupe_store.flush()

        self._notifier.info(
            "Run summary",
            imported=imported,
            exported=exported,
            dropped=dropped,
        )

        return OrchestratorResult(imported=imported, exported=exported, dropped=dropped)


# -----------------------------
# CLI / Composition Root
# -----------------------------

def _parse_csv_set(raw: str) -> Set[str]:
    if not raw.strip():
        return set()
    return {x.strip().upper() for x in raw.split(",") if x.strip()}


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Bridge unsupported financial exports into Simplifi import CSV")
    ap.add_argument("--import", dest="import_uri", required=True, help="Import URI/path (e.g. cashapp.csv or file:///... )")
    ap.add_argument("--export", dest="export_uri", required=True, help="Export URI/path for Simplifi CSV (e.g. out.csv)")
    ap.add_argument("--seen-rows", default="", help="Path to seen rows file (optional). Recommended to prevent duplicates across runs.")
    ap.add_argument("--rules", default="", help="Path to rules.json (optional).")
    ap.add_argument("--include-types", default="", help="Comma-separated Cash App Transaction Types to include (default: ALL)")
    ap.add_argument("--status", default="COMPLETE", help="Comma-separated Cash App Status values to include (default: COMPLETE)")
    ap.add_argument("--dry-run", action="store_true", help="Import + transform, but do not write export file")
    ap.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = ap.parse_args(argv)

    buffered = BufferedSink()
    console = ConsoleSink(min_level=LogLevel.DEBUG if args.debug else LogLevel.INFO, enable_color=True)
    notifier = Notifier([buffered, console])

    import_uri = Uri(args.import_uri)
    export_uri = Uri(args.export_uri)

    include_types = _parse_csv_set(args.include_types)
    statuses = _parse_csv_set(args.status)

    # Seen rows store is optional but strongly recommended.
    seen_rows_path = args.seen_rows.strip()
    dedupe_store: Optional[IRowDedupeStore] = None
    if seen_rows_path:
        dedupe_store = FileRowDedupeStore(Path(seen_rows_path).expanduser().resolve(), notifier)

    export_path = export_uri.as_path()
    skip_report_path = export_path.with_name(export_path.name + ".skipped.csv")

    import_context = ImportContext(
        notifier=notifier,
        source=import_uri,
        dedupe_store=dedupe_store,
        options={"skip_report_path": str(skip_report_path)},
    )
    export_context = ExportContext(notifier=notifier, destination=export_uri)

    # Build transformer pipeline
    transformers: List[ITransformer] = []
    if args.rules.strip():
        transformers.append(RulesTransformer.load_from_json(Path(args.rules).expanduser().resolve(), notifier))
    pipeline_transformer: Optional[ITransformer] = PipelineTransformer(transformers) if transformers else None

    # Register importers/exporters (extensible; add more without changing orchestrator)
    importer_registry = ImporterRegistry([
        CashAppCsvImporter(include_types=include_types, require_status=statuses),
        # Future: VenmoCsvImporter(), PaypalCsvImporter(), ApiImporter(), etc.
    ])
    exporter_registry = ExporterRegistry([
        SimplifiCsvExporter(),
        # Future: QifExporter(), OtherCsvExporter(), ApiExporter(), etc.
    ])

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
        notifier.critical("Fatal error", error=type(ex).__name__, message=str(ex))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
