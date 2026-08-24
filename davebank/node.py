"""A single DaveBank node: the peer-to-peer middleware.

Each node is an autonomous peer that:

  * has a stable unique id (UUID) and a human-friendly nickname,
  * discovers and tracks other peers over UDP (broadcast + gossip + an
    explicit seed peer),
  * participates in a **sequencer-based total-order multicast** so that every
    transaction is applied in the same order on every node,
  * elects the live peer with the lowest id as the sequencer, re-electing
    automatically when that peer dies (so the network survives ANY node
    failing - a true peer-to-peer property),
  * syncs the full ledger to newly-joined peers and recovers dropped messages
    via NACKs,
  * can simulate unreliable links by probabilistically dropping packets.

Only the Python standard library is used (`socket`, `threading`, `json`,
`uuid`). No messaging middleware is involved - the coordination logic here IS
the middleware.
"""

from __future__ import annotations

import base64
import hashlib
import os
import random
import socket
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple

from . import protocol as P
from .ledger import Ledger, Transaction

Addr = Tuple[str, int]

# Raw bytes per file chunk. Base64 inflates this by ~4/3, so 30 KB -> ~40 KB,
# leaving headroom under the 60 KB UDP datagram cap for the JSON envelope.
FILE_CHUNK_BYTES = 30000

_CONTENT_TYPES = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".gif": "image/gif", ".pdf": "application/pdf", ".txt": "text/plain",
    ".json": "application/json", ".csv": "text/csv", ".zip": "application/zip",
}


def _guess_content_type(name: str) -> str:
    _, ext = os.path.splitext(name.lower())
    return _CONTENT_TYPES.get(ext, "application/octet-stream")

# Timing constants (seconds).
HEARTBEAT_INTERVAL = 2.0     # how often we announce liveness
PEER_TIMEOUT = 7.0           # drop a peer unheard-from for this long
MAINTENANCE_TICK = 1.0       # how often the maintenance loop runs

DEFAULT_DISCOVERY_PORT = 50505   # shared port used only for broadcast HELLOs
DEFAULT_NETWORK = "davebank"     # logical network name; nodes only join their own
NETWORK_TTL = 15.0               # forget a foreign network unheard-from this long


class Peer:
    __slots__ = ("node_id", "nickname", "addr", "last_seen")

    def __init__(self, node_id: str, nickname: str, addr: Addr, last_seen: float):
        self.node_id = node_id
        self.nickname = nickname
        self.addr = addr
        self.last_seen = last_seen


class Node:
    def __init__(
        self,
        nickname: str,
        host: str = "0.0.0.0",
        port: int = 0,
        discovery_port: int = DEFAULT_DISCOVERY_PORT,
        seed: Optional[Addr] = None,
        loss: float = 0.0,
        on_event=None,
        network: str = DEFAULT_NETWORK,
    ) -> None:
        self.node_id = str(uuid.uuid4())
        self.nickname = nickname
        self.host = host
        self.discovery_port = discovery_port
        self.seed = seed
        self.network = network or DEFAULT_NETWORK
        self.loss = max(0.0, min(1.0, loss))   # simulated packet-loss rate
        self._on_event = on_event or (lambda msg: None)

        # Main unicast socket - the node's real address on the network.
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._sock.bind((host, port))
        self.port = self._sock.getsockname()[1]

        # Separate socket that listens for broadcast HELLOs on a shared port.
        self._disc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._disc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self._disc_sock.bind(("", discovery_port))
            self._disc_enabled = True
        except OSError:
            # Broadcast discovery is a bonus; an explicit --peer seed always
            # works even if we cannot grab the shared discovery port.
            self._disc_enabled = False

        self.ledger = Ledger()

        self._peers: Dict[str, Peer] = {}
        self._peers_lock = threading.RLock()

        # Other logical networks we have overheard on the shared discovery port
        # (name -> {"nodes": set(node_id), "last_seen": ts}). Lets a user see
        # which networks exist to join (start with --network NAME to join one).
        self._networks: Dict[str, dict] = {}
        self._networks_lock = threading.RLock()

        # Total-order delivery state.
        self._holdback: Dict[int, Transaction] = {}   # seq -> txn not yet applied
        self._order_lock = threading.RLock()
        self._assign_high = 0          # next seq this node would assign as sequencer
        self._origin_counter = 0       # local counter -> unique item ids
        self._seen_high = 0            # highest seq we've heard exists anywhere

        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        self._spawn(self._recv_loop)
        if self._disc_enabled:
            self._spawn(self._disc_loop)
        self._spawn(self._maintenance_loop)
        # Announce ourselves: broadcast for zero-config LAN discovery, and
        # unicast to an explicit seed peer if one was supplied.
        self._broadcast_hello()
        if self.seed:
            self._send(self.seed, self._hello_msg())
            # Ask the seed for the current ledger so we converge on join.
            self._send(self.seed, {**self._base_msg(P.STATE_REQUEST)})

    def stop(self) -> None:
        if self._stop.is_set():
            return
        # Announce departure BEFORE setting the stop flag: _send() short-circuits
        # once _stop is set, so broadcasting after would silently suppress the
        # GOODBYE and peers would only notice us via the slower heartbeat
        # timeout. Sending first lets them prune us immediately (graceful).
        self._broadcast({**self._base_msg(P.GOODBYE)})
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass
        try:
            self._disc_sock.close()
        except OSError:
            pass

    def _spawn(self, target) -> None:
        t = threading.Thread(target=target, daemon=True)
        t.start()
        self._threads.append(t)

    # ------------------------------------------------------------------ #
    # Low-level send / receive (with simulated loss)
    # ------------------------------------------------------------------ #
    def _send(self, addr: Addr, message: dict) -> None:
        if self._stop.is_set():
            return
        if self.loss and random.random() < self.loss:
            return   # simulate an unreliable outbound link by dropping
        try:
            self._sock.sendto(P.encode(message), addr)
        except OSError:
            pass

    def _broadcast(self, message: dict) -> None:
        """Unicast a message to every known peer (application-level multicast)."""
        for addr in self._peer_addrs():
            self._send(addr, message)

    def _broadcast_hello(self) -> None:
        if not self._disc_enabled:
            return
        try:
            self._sock.sendto(
                P.encode(self._hello_msg()),
                ("255.255.255.255", self.discovery_port),
            )
        except OSError:
            pass

    # ------------------------------------------------------------------ #
    # Message construction helpers
    # ------------------------------------------------------------------ #
    def _base_msg(self, mtype: str) -> dict:
        return {"type": mtype, "node_id": self.node_id,
                "nickname": self.nickname, "net": self.network}

    def _hello_msg(self) -> dict:
        return {**self._base_msg(P.HELLO), "port": self.port}

    # ------------------------------------------------------------------ #
    # Receive loops
    # ------------------------------------------------------------------ #
    def _recv_loop(self) -> None:
        while not self._stop.is_set():
            try:
                data, addr = self._sock.recvfrom(P.MAX_DATAGRAM)
            except OSError:
                # On Windows a datagram sent to a since-departed peer bounces
                # back an ICMP port-unreachable, surfacing here as
                # ConnectionResetError (WSAECONNRESET). That is NOT fatal - the
                # server must keep receiving from everyone else. Only exit when
                # we are actually shutting down (socket closed).
                if self._stop.is_set():
                    break
                continue
            if self.loss and random.random() < self.loss:
                continue   # simulate an unreliable inbound link
            self._handle(data, addr)

    def _disc_loop(self) -> None:
        while not self._stop.is_set():
            try:
                data, addr = self._disc_sock.recvfrom(P.MAX_DATAGRAM)
            except OSError:
                if self._stop.is_set():
                    break
                continue
            self._handle(data, addr)

    def _handle(self, data: bytes, addr: Addr) -> None:
        msg = P.decode(data)
        if msg is None:
            return                       # malformed packet -> dropped gracefully
        if msg["node_id"] == self.node_id:
            return                       # ignore our own broadcasts
        # Networks are isolated: a message from a different logical network is
        # not one of our peers. We still note the network so a user can see
        # which networks are out there to join.
        if msg.get("net", DEFAULT_NETWORK) != self.network:
            self._note_foreign_network(msg)
            return
        self._touch_peer(msg, addr)

        mtype = msg["type"]
        handler = {
            P.HELLO: self._on_hello,
            P.WELCOME: self._on_welcome,
            P.HEARTBEAT: self._on_heartbeat,
            P.GOODBYE: self._on_goodbye,
            P.SUBMIT: self._on_submit,
            P.ORDERED: self._on_ordered,
            P.STATE_REQUEST: self._on_state_request,
            P.STATE_RESPONSE: self._on_state_response,
            P.NACK: self._on_nack,
        }.get(mtype)
        if handler:
            try:
                handler(msg, addr)
            except Exception:
                # A single bad message must never take the node down.
                pass

    # ------------------------------------------------------------------ #
    # Membership
    # ------------------------------------------------------------------ #
    def _peer_main_addr(self, msg: dict, addr: Addr) -> Addr:
        """Resolve a peer's *main* unicast address.

        HELLO carries the advertised main port; for other message types the
        source address is already the main socket.
        """
        port = msg.get("port")
        if isinstance(port, int) and 0 < port < 65536:
            return (addr[0], port)
        return addr

    def _touch_peer(self, msg: dict, addr: Addr) -> None:
        node_id = msg["node_id"]
        nickname = msg.get("nickname", "?")
        main_addr = self._peer_main_addr(msg, addr)
        with self._peers_lock:
            peer = self._peers.get(node_id)
            if peer is None:
                # Don't register a peer purely from a broadcast HELLO's source
                # port; use the advertised main address.
                self._peers[node_id] = Peer(node_id, nickname, main_addr, time.time())
                self._on_event(f"+ peer joined: {nickname} ({node_id[:8]}) @ {main_addr[0]}:{main_addr[1]}")
            else:
                peer.last_seen = time.time()
                peer.nickname = nickname
                if msg["type"] in (P.HELLO, P.WELCOME):
                    peer.addr = main_addr

    def _remove_peer(self, node_id: str, reason: str) -> None:
        with self._peers_lock:
            peer = self._peers.pop(node_id, None)
        if peer:
            self._on_event(f"- peer left ({reason}): {peer.nickname} ({node_id[:8]})")

    def _peer_addrs(self) -> List[Addr]:
        with self._peers_lock:
            return [p.addr for p in self._peers.values()]

    def peers_view(self) -> List[Tuple[str, str, Addr]]:
        with self._peers_lock:
            return [(p.node_id, p.nickname, p.addr) for p in self._peers.values()]

    # ------------------------------------------------------------------ #
    # Network discovery (which other logical networks exist to join)
    # ------------------------------------------------------------------ #
    def _note_foreign_network(self, msg: dict) -> None:
        net = msg.get("net", DEFAULT_NETWORK)
        with self._networks_lock:
            entry = self._networks.setdefault(net, {"nodes": set(), "last_seen": 0.0})
            entry["nodes"].add(msg["node_id"])
            entry["last_seen"] = time.time()

    def _prune_networks(self) -> None:
        now = time.time()
        with self._networks_lock:
            for net in [n for n, e in self._networks.items()
                        if now - e["last_seen"] > NETWORK_TTL]:
                del self._networks[net]

    def discovered_networks(self) -> List[Tuple[str, int]]:
        """Other networks overheard on the LAN: list of (name, node count)."""
        with self._networks_lock:
            return sorted((n, len(e["nodes"])) for n, e in self._networks.items())

    # ------------------------------------------------------------------ #
    # Sequencer election (lowest live node id wins, deterministically)
    # ------------------------------------------------------------------ #
    def _sequencer_id(self) -> str:
        with self._peers_lock:
            ids = [self.node_id] + list(self._peers.keys())
        return min(ids)

    def _sequencer_addr(self) -> Optional[Addr]:
        sid = self._sequencer_id()
        if sid == self.node_id:
            return None              # we are the sequencer
        with self._peers_lock:
            peer = self._peers.get(sid)
            return peer.addr if peer else None

    def is_sequencer(self) -> bool:
        return self._sequencer_id() == self.node_id

    # ------------------------------------------------------------------ #
    # Discovery / membership message handlers
    # ------------------------------------------------------------------ #
    def _roster_payload(self, exclude_id: Optional[str] = None) -> List[dict]:
        with self._peers_lock:
            return [
                {"node_id": p.node_id, "nickname": p.nickname,
                 "ip": p.addr[0], "port": p.addr[1]}
                for p in self._peers.values()
                if p.node_id != exclude_id
            ]

    def _learn_roster(self, entries) -> None:
        """Learn about peers we don't yet know from a gossiped roster, and
        introduce ourselves directly.

        This is called from both WELCOME and HEARTBEAT handling, not just
        once on join: a single missed introduction (e.g. two peers joining
        the same seed at nearly the same instant, so neither's WELCOME
        roster yet contained the other) would otherwise leave the mesh
        permanently split, since nothing else re-triggers it.
        """
        for entry in entries:
            try:
                nid = entry["node_id"]
                paddr = (entry["ip"], int(entry["port"]))
            except (KeyError, TypeError, ValueError):
                continue
            if nid == self.node_id:
                continue
            with self._peers_lock:
                known = nid in self._peers
            if not known:
                self._send(paddr, self._hello_msg())

    def _on_hello(self, msg: dict, addr: Addr) -> None:
        # Reply with the peers we know so the newcomer learns the whole network.
        roster = self._roster_payload(exclude_id=msg["node_id"])
        welcome = {**self._base_msg(P.WELCOME), "port": self.port, "peers": roster}
        self._send(self._peer_main_addr(msg, addr), welcome)

    def _on_welcome(self, msg: dict, addr: Addr) -> None:
        self._learn_roster(msg.get("peers", []))

    def _on_heartbeat(self, msg: dict, addr: Addr) -> None:
        # Heartbeats advertise the sender's highest sequence so we can detect
        # if we have fallen behind and recover via NACK. They also carry the
        # sender's roster so the mesh keeps self-healing every tick, not just
        # once at join time (see _learn_roster).
        high = msg.get("seq_high")
        if isinstance(high, int):
            self._note_seen_high(high)
            self._request_missing(self._peer_main_addr(msg, addr))
        peers = msg.get("peers")
        if isinstance(peers, list):
            self._learn_roster(peers)

    def _on_goodbye(self, msg: dict, addr: Addr) -> None:
        self._remove_peer(msg["node_id"], "graceful")

    # ------------------------------------------------------------------ #
    # Transaction submission + total-order multicast
    # ------------------------------------------------------------------ #
    def _next_item_id(self) -> str:
        self._origin_counter += 1
        return f"{self.node_id}:{self._origin_counter}"

    def _inject(self, txn: Transaction) -> None:
        """Push an already-built, validated transaction into the network."""
        if self.is_sequencer():
            self._assign_and_multicast(txn)
        else:
            seq_addr = self._sequencer_addr()
            if seq_addr:
                self._send(seq_addr, {**self._base_msg(P.SUBMIT), "txn": txn.to_dict()})
            else:
                # We think we're sequencer-less momentarily; order it ourselves.
                self._assign_and_multicast(txn)

    def submit(self, kind: str, account: str, amount: float,
               target: Optional[str] = None, note: str = "") -> str:
        """Create a transaction and inject it into the network. Returns item_id."""
        txn = Transaction(
            item_id=self._next_item_id(), kind=kind, account=account,
            amount=float(amount), target=target, note=note,
            origin_nickname=self.nickname, timestamp=time.time(),
        )
        # Validate locally up-front so the user gets immediate feedback.
        txn = Transaction.from_dict(txn.to_dict())
        self._inject(txn)
        return txn.item_id

    def submit_file(self, path: str, account: str = "") -> Tuple[str, str, int]:
        """Chunk a binary file and inject it as peer-to-peer data items.

        Returns (manifest_item_id, filename, chunk_count). The file replicates
        to every node through the normal totally-ordered path, where it can be
        reassembled and hash-verified. Raises OSError if the file is unreadable.
        """
        with open(path, "rb") as fh:
            data = fh.read()
        name = os.path.basename(path)
        sha = hashlib.sha256(data).hexdigest()
        pieces = [data[i:i + FILE_CHUNK_BYTES] for i in range(0, len(data), FILE_CHUNK_BYTES)] or [b""]

        manifest_id = self._next_item_id()
        manifest = Transaction.from_dict({
            "item_id": manifest_id, "kind": "file", "account": account,
            "amount": 0, "note": name, "origin_nickname": self.nickname,
            "timestamp": time.time(),
            "payload": {"name": name, "size": len(data), "sha256": sha,
                        "chunks": len(pieces),
                        "content_type": _guess_content_type(name)},
        })
        self._inject(manifest)

        for index, piece in enumerate(pieces):
            chunk = Transaction.from_dict({
                "item_id": self._next_item_id(), "kind": "chunk",
                "origin_nickname": self.nickname, "timestamp": time.time(),
                "payload": {"file_id": manifest_id, "index": index,
                            "data": base64.b64encode(piece).decode("ascii")},
            })
            self._inject(chunk)
        return manifest_id, name, len(pieces)

    def _on_submit(self, msg: dict, addr: Addr) -> None:
        try:
            txn = Transaction.from_dict(msg["txn"])
        except (KeyError, ValueError):
            return                      # malformed transaction -> dropped
        if self.is_sequencer():
            self._assign_and_multicast(txn)
        else:
            # Forward to whoever is actually the sequencer now.
            seq_addr = self._sequencer_addr()
            if seq_addr:
                self._send(seq_addr, {**self._base_msg(P.SUBMIT), "txn": txn.to_dict()})

    def _assign_and_multicast(self, txn: Transaction) -> None:
        """Sequencer path: stamp the txn with a global seq and multicast it."""
        if self.ledger.has_item(txn.item_id):
            return                      # never order the same data item twice
        with self._order_lock:
            self._assign_high = max(self._assign_high, self.ledger.expected_seq(),
                                    self._seen_high)
            txn.seq = self._assign_high
            self._assign_high += 1
        ordered = {**self._base_msg(P.ORDERED), "txn": txn.to_dict()}
        self._deliver(txn)              # apply to our own ledger
        self._broadcast(ordered)        # and to everyone else

    def _on_ordered(self, msg: dict, addr: Addr) -> None:
        try:
            txn = Transaction.from_dict(msg["txn"])
        except (KeyError, ValueError):
            return
        if txn.seq < 0:
            return
        self._note_seen_high(txn.seq + 1)
        self._deliver(txn)

    def _deliver(self, txn: Transaction) -> None:
        """Apply in total order, buffering gaps and recovering via NACK."""
        with self._order_lock:
            expected = self.ledger.expected_seq()
            if txn.seq < expected:
                return                  # already have it
            self._holdback[txn.seq] = txn
            # Drain as many contiguous transactions as we now hold.
            while True:
                nxt = self.ledger.expected_seq()
                held = self._holdback.pop(nxt, None)
                if held is None:
                    break
                self.ledger.apply(held)
                self._on_event(
                    f"#{held.seq} {held.kind} {held.account} {held.amount:g}"
                    + (f" -> {held.target}" if held.target else "")
                    + f"  (by {held.origin_nickname})"
                )
            # If a gap remains, ask for the missing pieces.
            if self._holdback:
                self._request_missing(addr=None)

    # ------------------------------------------------------------------ #
    # Recovery: NACK for missing sequence numbers
    # ------------------------------------------------------------------ #
    def _note_seen_high(self, high: int) -> None:
        with self._order_lock:
            if high > self._seen_high:
                self._seen_high = high

    def _missing_seqs(self) -> List[int]:
        with self._order_lock:
            expected = self.ledger.expected_seq()
            upper = max(self._seen_high, expected)
            held = set(self._holdback)
            return [s for s in range(expected, upper) if s not in held]

    def _request_missing(self, addr: Optional[Addr]) -> None:
        missing = self._missing_seqs()
        if not missing:
            return
        nack = {**self._base_msg(P.NACK), "seqs": missing[:200]}
        if addr is not None:
            self._send(addr, nack)
        else:
            seq_addr = self._sequencer_addr()
            if seq_addr:
                self._send(seq_addr, nack)
            else:
                # Ask a random peer if we are (or think we are) the sequencer.
                addrs = self._peer_addrs()
                if addrs:
                    self._send(random.choice(addrs), nack)

    def _on_nack(self, msg: dict, addr: Addr) -> None:
        seqs = msg.get("seqs", [])
        if not isinstance(seqs, list):
            return
        for s in seqs[:200]:
            if not isinstance(s, int):
                continue
            txn = self.ledger.get(s)
            if txn is not None:
                self._send(self._peer_main_addr(msg, addr),
                           {**self._base_msg(P.ORDERED), "txn": txn.to_dict()})

    # ------------------------------------------------------------------ #
    # State synchronisation (join + rebuild)
    # ------------------------------------------------------------------ #
    def _on_state_request(self, msg: dict, addr: Addr) -> None:
        snapshot = self.ledger.snapshot()
        # Batch the log so each datagram stays under the MTU. We budget by
        # *bytes*, not item count, because a single file chunk can be tens of
        # kilobytes while a plain transaction is tiny.
        budget = P.MAX_DATAGRAM - 2000     # leave room for the envelope
        batch: List[dict] = []
        nbytes = 0
        for item in snapshot:
            item_bytes = len(P.encode(item))
            if batch and nbytes + item_bytes > budget:
                self._send(addr, {**self._base_msg(P.STATE_RESPONSE), "log": batch})
                batch, nbytes = [], 0
            batch.append(item)
            nbytes += item_bytes
        # Always send a final (possibly empty) batch so the joiner knows the end.
        self._send(addr, {**self._base_msg(P.STATE_RESPONSE), "log": batch})

    def _on_state_response(self, msg: dict, addr: Addr) -> None:
        log = msg.get("log", [])
        if not isinstance(log, list):
            return
        for raw in log:
            try:
                txn = Transaction.from_dict(raw)
            except ValueError:
                continue
            if txn.seq >= 0:
                self._note_seen_high(txn.seq + 1)
                self._deliver(txn)

    def request_state(self) -> None:
        """Ask a peer (preferably the sequencer) for the full ledger."""
        addr = self._sequencer_addr()
        if addr is None:
            addrs = self._peer_addrs()
            addr = addrs[0] if addrs else None
        if addr is not None:
            self._send(addr, {**self._base_msg(P.STATE_REQUEST)})

    def rebuild_from_network(self) -> None:
        """Clear the local ledger and rebuild it from a peer ('could have')."""
        with self._order_lock:
            self.ledger.rebuild_from([])
            self._holdback.clear()
        self.request_state()

    # ------------------------------------------------------------------ #
    # Maintenance: heartbeats, peer pruning, gap recovery
    # ------------------------------------------------------------------ #
    def _maintenance_loop(self) -> None:
        last_hb = 0.0
        while not self._stop.wait(MAINTENANCE_TICK):
            now = time.time()
            if now - last_hb >= HEARTBEAT_INTERVAL:
                last_hb = now
                hb = {**self._base_msg(P.HEARTBEAT),
                      "seq_high": self.ledger.size(),
                      "peers": self._roster_payload()}
                self._broadcast(hb)
                self._broadcast_hello()        # keep discovering on the LAN
                with self._peers_lock:
                    no_peers_yet = not self._peers
                if self.seed and no_peers_yet:
                    # The initial join HELLO/STATE_REQUEST was sent exactly
                    # once in start(); a single lost UDP packet (very
                    # plausible for the very first packet between two
                    # machines) would otherwise leave us stuck forever with
                    # no peers. Keep retrying until the seed answers.
                    self._send(self.seed, self._hello_msg())
                    self._send(self.seed, {**self._base_msg(P.STATE_REQUEST)})
            # Prune peers we haven't heard from -> handles ungraceful death.
            dead = []
            with self._peers_lock:
                for nid, p in self._peers.items():
                    if now - p.last_seen > PEER_TIMEOUT:
                        dead.append(nid)
            for nid in dead:
                self._remove_peer(nid, "timeout")
            self._prune_networks()
            # Proactively recover any gaps in our log.
            self._request_missing(addr=None)
