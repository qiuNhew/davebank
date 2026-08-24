"""The replicated DaveBank ledger.

The ledger is an append-only log of transactions that have been assigned a
**global sequence number** by the network's sequencer. Because every node
applies exactly the same transactions in exactly the same sequence order, the
log (and therefore every account balance and every index) is identical on all
nodes. This is the core of the "complete accurate ordering ... consistent on
all nodes" requirement.

Balances are *derived* by replaying the log rather than stored, which keeps
the single source of truth unambiguous and makes "rebuild from the network"
trivial.

Supported transaction kinds (richer than simple deposit/withdraw, satisfying
the "more complex data items" requirement):

    deposit   - add ``amount`` to ``account``
    withdraw  - subtract ``amount`` from ``account``
    transfer  - move ``amount`` from ``account`` to ``target``

Binary files are carried as ordinary data items too (the "could have" feature:
transfer binary files as peer-to-peer data items). A file is represented by:

    file      - a *manifest* item: name, size, sha256, chunk count, linked
                account (in ``payload``)
    chunk     - one base64-encoded slice of a file: file_id, index, data
                (in ``payload``)

Because these flow through the very same totally-ordered, replicated log, a file
uploaded on one node is reassembled - and hash-verified - on every other node,
with dropped chunks recovered automatically by the node layer's NACK mechanism.
No client/server: the file *is* peer-to-peer data.
"""

from __future__ import annotations

import base64
import hashlib
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

FINANCIAL_KINDS = {"deposit", "withdraw", "transfer"}
FILE_KINDS = {"file", "chunk"}
VALID_KINDS = FINANCIAL_KINDS | FILE_KINDS


@dataclass
class Transaction:
    """A single financial data item on the network.

    item_id : globally-unique id of the data item, formed as
              ``"<origin_node_id>:<origin_counter>"``. Unique regardless of
              which node created it (satisfies "uniquely identify all data
              items").
    seq     : the network-wide index assigned by the sequencer. The same on
              every node (satisfies "show the same indexes for all network
              data on each node").
    """

    item_id: str
    kind: str
    account: str
    amount: float
    target: Optional[str] = None   # only used by 'transfer'
    note: str = ""
    origin_nickname: str = ""
    timestamp: float = 0.0
    seq: int = -1                  # filled in once ordered by the sequencer
    payload: Optional[dict] = None  # kind-specific extra data (file / chunk)

    def to_dict(self) -> dict:
        return {
            "item_id": self.item_id,
            "kind": self.kind,
            "account": self.account,
            "amount": self.amount,
            "target": self.target,
            "note": self.note,
            "origin_nickname": self.origin_nickname,
            "timestamp": self.timestamp,
            "seq": self.seq,
            "payload": self.payload,
        }

    @staticmethod
    def from_dict(d: dict) -> "Transaction":
        """Build a Transaction from an untrusted dict, validating fields.

        Raises ValueError if the data item is malformed so the caller can drop
        it gracefully.
        """
        kind = d.get("kind")
        if kind not in VALID_KINDS:
            raise ValueError(f"unknown transaction kind: {kind!r}")

        item_id = d.get("item_id")
        if not isinstance(item_id, str) or not item_id:
            raise ValueError("item_id must be a non-empty string")

        if kind in FILE_KINDS:
            return Transaction._file_from_dict(kind, item_id, d)

        # Financial kinds require an account and a non-negative numeric amount.
        account = d.get("account")
        if not isinstance(account, str) or not account.strip():
            raise ValueError("account must be a non-empty string")

        amount = d.get("amount")
        if not isinstance(amount, (int, float)) or isinstance(amount, bool):
            raise ValueError("amount must be numeric")
        if amount < 0:
            raise ValueError("amount must not be negative")

        target = d.get("target")
        if kind == "transfer":
            if not isinstance(target, str) or not target.strip():
                raise ValueError("transfer requires a target account")
        else:
            target = None

        return Transaction(
            item_id=item_id,
            kind=kind,
            account=account.strip(),
            amount=float(amount),
            target=target.strip() if isinstance(target, str) else None,
            note=str(d.get("note", ""))[:200],
            origin_nickname=str(d.get("origin_nickname", "")),
            timestamp=float(d.get("timestamp", 0.0)),
            seq=int(d.get("seq", -1)),
        )

    @staticmethod
    def _file_from_dict(kind: str, item_id: str, d: dict) -> "Transaction":
        """Validate a file-manifest or file-chunk data item."""
        p = d.get("payload")
        if not isinstance(p, dict):
            raise ValueError("file/chunk item requires a payload object")

        if kind == "file":
            name = p.get("name")
            size = p.get("size")
            sha = p.get("sha256")
            chunks = p.get("chunks")
            if not isinstance(name, str) or not name:
                raise ValueError("file manifest requires a name")
            if not isinstance(size, int) or size < 0:
                raise ValueError("file manifest requires a non-negative size")
            if not (isinstance(sha, str) and len(sha) == 64):
                raise ValueError("file manifest requires a sha256 hex digest")
            if not isinstance(chunks, int) or chunks < 0:
                raise ValueError("file manifest requires a chunk count")
            payload = {"name": name, "size": size, "sha256": sha,
                       "chunks": chunks,
                       "content_type": str(p.get("content_type", "application/octet-stream"))}
        else:  # chunk
            file_id = p.get("file_id")
            index = p.get("index")
            data = p.get("data")
            if not isinstance(file_id, str) or not file_id:
                raise ValueError("chunk requires a file_id")
            if not isinstance(index, int) or index < 0:
                raise ValueError("chunk requires a non-negative index")
            if not isinstance(data, str):
                raise ValueError("chunk requires base64 data")
            payload = {"file_id": file_id, "index": index, "data": data}

        return Transaction(
            item_id=item_id,
            kind=kind,
            account=str(d.get("account", "")),
            amount=0.0,
            target=None,
            note=str(d.get("note", ""))[:200],
            origin_nickname=str(d.get("origin_nickname", "")),
            timestamp=float(d.get("timestamp", 0.0)),
            seq=int(d.get("seq", -1)),
            payload=payload,
        )


class Ledger:
    """Thread-safe, totally-ordered transaction log with derived balances.

    The ledger only ever *applies* a transaction at the expected next sequence
    number. Out-of-order arrivals are buffered by the node layer (holdback
    queue) and replayed here in order, guaranteeing identical state everywhere.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._log: List[Transaction] = []          # index position == seq
        self._balances: Dict[str, float] = {}
        self._item_ids: set = set()                 # for duplicate detection

    # -- application -------------------------------------------------------
    def expected_seq(self) -> int:
        """The next contiguous sequence number this ledger wants to apply."""
        with self._lock:
            return len(self._log)

    def has_item(self, item_id: str) -> bool:
        with self._lock:
            return item_id in self._item_ids

    def apply(self, txn: Transaction) -> bool:
        """Apply ``txn`` iff its seq is exactly the next expected one.

        Returns True if applied, False if it was a duplicate / out of order
        (the caller decides whether to buffer it).
        """
        with self._lock:
            if txn.seq != len(self._log):
                return False
            if txn.item_id in self._item_ids:
                # Same data item delivered twice; ignore but treat as applied
                # so the sequence can advance is *not* what we want -- a
                # duplicate at the expected slot should never happen, so guard.
                return False
            self._log.append(txn)
            self._item_ids.add(txn.item_id)
            self._mutate_balances(txn)
            return True

    def _mutate_balances(self, txn: Transaction) -> None:
        if txn.kind == "deposit":
            self._balances[txn.account] = self._balances.get(txn.account, 0.0) + txn.amount
        elif txn.kind == "withdraw":
            self._balances[txn.account] = self._balances.get(txn.account, 0.0) - txn.amount
        elif txn.kind == "transfer":
            self._balances[txn.account] = self._balances.get(txn.account, 0.0) - txn.amount
            self._balances[txn.target] = self._balances.get(txn.target, 0.0) + txn.amount

    # -- queries -----------------------------------------------------------
    def balance(self, account: str) -> float:
        with self._lock:
            return self._balances.get(account, 0.0)

    def all_balances(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._balances)

    def get(self, seq: int) -> Optional[Transaction]:
        """Retrieve a data item by its network index (sequence number)."""
        with self._lock:
            if 0 <= seq < len(self._log):
                return self._log[seq]
            return None

    def snapshot(self) -> List[dict]:
        """A copy of the whole ordered log, for syncing a joining node."""
        with self._lock:
            return [t.to_dict() for t in self._log]

    def size(self) -> int:
        with self._lock:
            return len(self._log)

    # -- binary files ------------------------------------------------------
    def file_manifests(self) -> List[Tuple[int, Transaction, int]]:
        """List uploaded files: (manifest seq, manifest txn, chunks present)."""
        with self._lock:
            present: Dict[str, int] = {}
            for t in self._log:
                if t.kind == "chunk":
                    fid = t.payload["file_id"]
                    present[fid] = present.get(fid, 0) + 1
            out = []
            for t in self._log:
                if t.kind == "file":
                    out.append((t.seq, t, present.get(t.item_id, 0)))
            return out

    def file_bytes(self, manifest_seq: int) -> Tuple[Transaction, bytes]:
        """Reassemble a file from its chunks and verify its SHA-256.

        Raises ValueError if there is no such file, chunks are missing, or the
        content hash does not match the manifest.
        """
        with self._lock:
            manifest = self._log[manifest_seq] if 0 <= manifest_seq < len(self._log) else None
            if manifest is None or manifest.kind != "file":
                raise ValueError(f"no file at index {manifest_seq}")
            file_id = manifest.item_id
            expected = manifest.payload["chunks"]
            pieces: Dict[int, str] = {}
            for t in self._log:
                if t.kind == "chunk" and t.payload["file_id"] == file_id:
                    pieces[t.payload["index"]] = t.payload["data"]
            if len(pieces) < expected:
                raise ValueError(
                    f"file incomplete: {len(pieces)}/{expected} chunks received "
                    f"(try again once the rest arrive)")
            raw = b"".join(base64.b64decode(pieces[i]) for i in range(expected))
        if hashlib.sha256(raw).hexdigest() != manifest.payload["sha256"]:
            raise ValueError("hash mismatch - file is corrupt")
        return manifest, raw

    def rebuild_from(self, items: List[dict]) -> None:
        """Discard local state and rebuild it from an ordered list of items.

        Used both when a fresh node joins and for the "clear local copy and
        rebuild from the network" feature. Items must be sorted by seq.
        """
        with self._lock:
            self._log.clear()
            self._balances.clear()
            self._item_ids.clear()
            for raw in sorted(items, key=lambda d: d.get("seq", 0)):
                txn = Transaction.from_dict(raw)
                if txn.seq == len(self._log):
                    self._log.append(txn)
                    self._item_ids.add(txn.item_id)
                    self._mutate_balances(txn)
