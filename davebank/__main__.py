"""DaveBank command-line interface.

Run a node, then drive it interactively:

    python -m davebank --nick alice
    python -m davebank --nick bob --peer 127.0.0.1:50000

Type ``help`` at the prompt for the command list.
"""

from __future__ import annotations

import argparse
import random
import threading
import time
from typing import Optional

from .node import DEFAULT_DISCOVERY_PORT, DEFAULT_NETWORK, Node
from .external import ExternalAPI

HELP = """\
DaveBank commands
  help                         show this help
  whoami                       show this node's id, nickname, address, role
  peers                        list known peers and who the sequencer is
  networks                     list other DaveBank networks discovered on the LAN
  deposit  <account> <amount> [note]
  withdraw <account> <amount> [note]
  transfer <from> <to> <amount> [note]
  balance  [account]           show one balance (or all if omitted)
  balances                     show every account balance
  get <index>                  retrieve the data item at a network index (seq)
  log                          list every data item by index (the shared index)
  putfile <path> [account]     upload a binary file into the network (P2P data items)
  files                        list uploaded files and their status
  getfile <index> <out-path>   reassemble + verify a file from the network by index
  robot start [interval]       auto-generate example data (default 1.5s)
  robot stop                   stop the robot
  loss <0..1>                  set simulated packet-loss rate on this node
  sync                         re-request the full ledger from a peer
  rebuild                      clear local ledger and rebuild from the network
  quit / exit                  graceful disconnect and leave
"""

ROBOT_ACCOUNTS = ["alice", "bob", "carol", "dave", "enron", "lehman"]


class Robot:
    """Background generator of example transactions ('robot' requirement)."""

    def __init__(self, node: Node):
        self.node = node
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.interval = 1.5

    def start(self, interval: float) -> None:
        if self._thread and self._thread.is_alive():
            return
        self.interval = max(0.2, interval)
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive() and not self._stop.is_set())

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            kind = random.choice(["deposit", "withdraw", "transfer"])
            amount = round(random.uniform(1, 500), 2)
            acct = random.choice(ROBOT_ACCOUNTS)
            if kind == "transfer":
                target = random.choice([a for a in ROBOT_ACCOUNTS if a != acct])
                self.node.submit("transfer", acct, amount, target=target, note="robot")
            else:
                self.node.submit(kind, acct, amount, note="robot")


def parse_seed(value: Optional[str]):
    if not value:
        return None
    host, _, port = value.partition(":")
    return (host, int(port))


def main() -> None:
    ap = argparse.ArgumentParser(prog="davebank", description="DaveBank P2P node")
    ap.add_argument("--nick", default=None, help="friendly nickname for this node")
    ap.add_argument("--host", default="0.0.0.0", help="bind host (default all)")
    ap.add_argument("--port", type=int, default=0, help="UDP port (0 = auto)")
    ap.add_argument("--peer", default=None, help="seed peer HOST:PORT to join")
    ap.add_argument("--discovery-port", type=int, default=DEFAULT_DISCOVERY_PORT,
                    help="shared broadcast discovery port")
    ap.add_argument("--network", default=DEFAULT_NETWORK,
                    help="logical network name to join (isolates node groups)")
    ap.add_argument("--api-port", type=int, default=0,
                    help="enable the external HTTP interface on this port (0=off)")
    ap.add_argument("--loss", type=float, default=0.0,
                    help="simulated packet-loss rate 0..1")
    ap.add_argument("--robot", action="store_true",
                    help="start the example-data robot immediately")
    args = ap.parse_args()

    nick = args.nick or f"node-{random.randint(1000, 9999)}"
    events: list = []

    def on_event(msg: str) -> None:
        # Buffer node events so they don't tangle with the input line; the
        # prompt loop flushes them between commands.
        events.append(msg)

    node = Node(
        nickname=nick, host=args.host, port=args.port,
        discovery_port=args.discovery_port, seed=parse_seed(args.peer),
        loss=args.loss, on_event=on_event, network=args.network,
    )
    node.start()
    robot = Robot(node)
    if args.robot:
        robot.start(1.5)

    api = None
    if args.api_port > 0:
        api = ExternalAPI(node, args.api_port)
        api.start()

    print(f"DaveBank node '{nick}' up: id={node.node_id[:8]} "
          f"udp={node.port} net='{node.network}' "
          f"discovery={'on' if node._disc_enabled else 'off'}")
    if api:
        print(f"external HTTP API on http://localhost:{args.api_port} "
              f"(POST /txn, GET /balances, GET /log)")
    if args.peer:
        print(f"joining via seed peer {args.peer}")
    print("type 'help' for commands\n")

    def flush_events() -> None:
        while events:
            print("  ·", events.pop(0))

    try:
        while True:
            time.sleep(0.05)
            flush_events()
            try:
                line = input(f"{nick}> ").strip()
            except EOFError:
                break
            if not line:
                continue
            parts = line.split()
            cmd, rest = parts[0].lower(), parts[1:]

            try:
                if cmd in ("quit", "exit"):
                    break
                elif cmd == "help":
                    print(HELP)
                elif cmd == "whoami":
                    role = "SEQUENCER" if node.is_sequencer() else "follower"
                    print(f"  nickname : {node.nickname}")
                    print(f"  node id  : {node.node_id}")
                    print(f"  address  : udp {node.port}")
                    print(f"  network  : {node.network}")
                    print(f"  role     : {role}")
                    print(f"  log size : {node.ledger.size()} items")
                    print(f"  api      : {'http://localhost:'+str(args.api_port) if api else 'off'}")
                    print(f"  loss sim : {node.loss}")
                elif cmd == "peers":
                    seq_id = node._sequencer_id()
                    me = " (me)" if seq_id == node.node_id else ""
                    print(f"  sequencer: {seq_id[:8]}{me}")
                    view = node.peers_view()
                    if not view:
                        print("  (no other peers known yet)")
                    for nid, nn, addr in view:
                        star = " *seq" if nid == seq_id else ""
                        print(f"  - {nn:<12} {nid[:8]} @ {addr[0]}:{addr[1]}{star}")
                elif cmd == "networks":
                    nets = node.discovered_networks()
                    print(f"  this node's network: {node.network}")
                    if not nets:
                        print("  (no other networks discovered)")
                    for name, count in nets:
                        print(f"  - {name:<16} {count} node(s)  "
                              f"(join with --network {name})")
                elif cmd in ("deposit", "withdraw"):
                    acct, amount = rest[0], float(rest[1])
                    note = " ".join(rest[2:])
                    iid = node.submit(cmd, acct, amount, note=note)
                    print(f"  submitted {cmd} {amount:g} -> {acct} (item {iid[-8:]})")
                elif cmd == "transfer":
                    src, dst, amount = rest[0], rest[1], float(rest[2])
                    note = " ".join(rest[3:])
                    iid = node.submit("transfer", src, amount, target=dst, note=note)
                    print(f"  submitted transfer {amount:g} {src}->{dst} (item {iid[-8:]})")
                elif cmd == "balance":
                    if rest:
                        print(f"  {rest[0]}: {node.ledger.balance(rest[0]):.2f}")
                    else:
                        _print_balances(node)
                elif cmd == "balances":
                    _print_balances(node)
                elif cmd == "get":
                    seq = int(rest[0])
                    txn = node.ledger.get(seq)
                    if txn is None:
                        print(f"  no data item at index {seq}")
                    elif txn.kind in ("file", "chunk"):
                        print(f"  #{txn.seq} item={txn.item_id[-8:]} {txn.kind}  "
                              + _describe_payload(txn)
                              + f"  by {txn.origin_nickname}")
                    else:
                        print(f"  #{txn.seq} item={txn.item_id[-8:]} {txn.kind} "
                              f"{txn.account} {txn.amount:g}"
                              + (f" -> {txn.target}" if txn.target else "")
                              + f"  note='{txn.note}' by {txn.origin_nickname}")
                elif cmd == "log":
                    n = node.ledger.size()
                    if n == 0:
                        print("  (empty)")
                    for s in range(n):
                        t = node.ledger.get(s)
                        if t.kind in ("file", "chunk"):
                            print(f"  #{s:<4} {t.kind:<8} {_describe_payload(t)}")
                        else:
                            print(f"  #{s:<4} {t.kind:<8} {t.account:<8} {t.amount:>10.2f}"
                                  + (f" -> {t.target}" if t.target else ""))
                elif cmd == "putfile":
                    path = rest[0]
                    account = rest[1] if len(rest) > 1 else ""
                    mid, name, nchunks = node.submit_file(path, account)
                    print(f"  uploading '{name}' as {nchunks} chunk(s) "
                          f"(item {mid[-8:]}); run 'files' to see its index")
                elif cmd == "files":
                    mans = node.ledger.file_manifests()
                    if not mans:
                        print("  (no files uploaded)")
                    for seq, m, present in mans:
                        total = m.payload["chunks"]
                        state = "complete" if present >= total else f"{present}/{total} chunks"
                        acct = f" acct={m.account}" if m.account else ""
                        print(f"  #{seq:<4} {m.payload['name']:<24} "
                              f"{m.payload['size']:>9} bytes  [{state}]{acct}")
                elif cmd == "getfile":
                    seq = int(rest[0])
                    out = rest[1]
                    manifest, data = node.ledger.file_bytes(seq)
                    with open(out, "wb") as fh:
                        fh.write(data)
                    print(f"  wrote {len(data)} bytes to '{out}' "
                          f"(sha256 verified OK: {manifest.payload['sha256'][:12]}...)")
                elif cmd == "robot":
                    if rest and rest[0] == "start":
                        interval = float(rest[1]) if len(rest) > 1 else 1.5
                        robot.start(interval)
                        print(f"  robot started (every {interval:g}s)")
                    elif rest and rest[0] == "stop":
                        robot.stop()
                        print("  robot stopped")
                    else:
                        print(f"  robot is {'running' if robot.running else 'stopped'}")
                elif cmd == "loss":
                    node.loss = max(0.0, min(1.0, float(rest[0])))
                    print(f"  packet-loss simulation set to {node.loss}")
                elif cmd == "sync":
                    node.request_state()
                    print("  requested full ledger from a peer")
                elif cmd == "rebuild":
                    node.rebuild_from_network()
                    print("  cleared local ledger; rebuilding from network")
                else:
                    print(f"  unknown command '{cmd}' (try 'help')")
            except (IndexError, ValueError) as e:
                print(f"  bad arguments: {e}")
            except OSError as e:
                print(f"  file error: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        print("\nleaving network...")
        robot.stop()
        if api:
            api.stop()
        node.stop()


def _describe_payload(txn) -> str:
    """One-line summary of a file/chunk data item's payload."""
    p = txn.payload or {}
    if txn.kind == "file":
        return f"{p.get('name','?')} ({p.get('size',0)} bytes, {p.get('chunks',0)} chunks)"
    return f"file={p.get('file_id','?')[-8:]} chunk#{p.get('index','?')}"


def _print_balances(node: Node) -> None:
    balances = node.ledger.all_balances()
    if not balances:
        print("  (no accounts yet)")
        return
    for acct in sorted(balances):
        print(f"  {acct:<12} {balances[acct]:>12.2f}")


if __name__ == "__main__":
    main()
