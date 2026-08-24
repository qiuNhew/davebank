# DaveBank

A peer-to-peer distributed banking system where the middleware is written by
hand, directly on UDP datagrams. Accounts live on every node, transactions can
be submitted from any node, and every node reports identical balances and
identical data-item indexes — with no server, no message queue, no MPI or RMI,
and no third-party packages. Pure Python 3 standard library.

## The problem it solves

UDP gives you no delivery guarantee, no ordering guarantee and no connections.
A bank is a system where **order is the product**: if two nodes disagree about
whether the withdrawal or the deposit came first, they disagree about the
balance. So the interesting work here is closing that gap without reaching for
a framework that would close it for you.

DaveBank does it with **sequencer-based total-order multicast**:

1. One node — always the live node with the lowest id — acts as the
   **sequencer**. Any node submitting a transaction sends it there.
2. The sequencer stamps it with the next global sequence number and multicasts
   it to every peer.
3. Every node applies transactions **strictly in sequence order**, buffering
   out-of-order arrivals in a holdback queue until the gap ahead is filled.

Because every node applies the same items in the same order to the same starting
state, the log, the balances and the indexes are identical everywhere. That
sequence number doubles as the network-wide index used to retrieve any item.

The obvious objection to a sequencer is that it is a single point of failure.
It isn't one here: the role is recomputed continuously as `min(live node ids)`
from each node's own membership view, so **no election messages are exchanged
at all**. Kill the sequencer and the next-lowest id starts stamping within a
heartbeat, resuming numbering from the highest sequence it has seen.

## What it does

* **Total ordering** — one agreed sequence, so balances and indexes match on
  every node.
* **Fault tolerance** — any node can die, including the sequencer; survivors
  re-elect and carry on. Graceful departures announce themselves; crashes are
  caught by heartbeat timeout.
* **Self-healing membership** — nodes are found by UDP broadcast, gossiped peer
  rosters and an optional seed peer. None of these mechanisms is one-shot: every
  heartbeat re-runs discovery, so a partitioned mesh repairs itself in seconds
  rather than depending on one lucky first packet.
* **Recovery over an unreliable link** — heartbeats advertise log height; a node
  that has fallen behind NACKs for the sequence numbers it lacks, and *any* peer
  can answer because every peer holds the whole log.
* **Failure simulation** — `--loss 0.5` drops half the packets on demand, so
  recovery is demonstrable and testable rather than theoretical.
* **Join and rebuild** — a new node is sent the entire ledger on join; `rebuild`
  wipes local state and reconstructs it from the network.
* **Binary files as peer-to-peer data items** — files ride the same ordered log
  as money (see below).
* **Logical networks** — `--network name` isolates node groups, while other
  networks on the LAN remain discoverable.
* **External HTTP interface** — post transactions in from outside the network.
* **Robustness** — malformed or hostile packets are validated and dropped; a bad
  datagram cannot crash a node.

## Quick start

Requires Python 3.8+. No dependencies to install.

```bash
# first node (becomes the initial sequencer)
python -m davebank --nick alice --port 50000

# more nodes, joining via a known peer — works across machines
python -m davebank --nick bob   --peer 127.0.0.1:50000
python -m davebank --nick carol --peer 127.0.0.1:50000
```

On a LAN nodes also auto-discover each other by UDP broadcast, so `--peer` is
optional there. Across machines, use the real IP: `--peer 10.0.1.23:50000`.

Then try this — it is the whole idea in five lines:

```
alice> deposit alice 100
alice> transfer alice bob 30
bob>   balances            # same numbers as alice
carol> log                 # same items, same #indexes, on a third machine
carol> get 1               # any item by its network-wide index
```

Now kill alice with Ctrl-C and run `peers` on bob: a new sequencer has been
elected, and submitting more transactions still works.

### Flags

| flag | meaning |
|------|---------|
| `--nick NAME` | friendly nickname shown to all peers |
| `--port N` | fixed UDP port (default: OS-assigned) |
| `--peer HOST:PORT` | seed peer to join an existing network |
| `--loss 0..1` | simulate an unreliable link by dropping this fraction of packets |
| `--robot` | start auto-generating example transactions immediately |
| `--network NAME` | join a named logical network |
| `--api-port N` | expose the HTTP interface on port N (start-up only) |

### Console commands

```
deposit <account> <amount> [note]     balance [account]      peers
withdraw <account> <amount> [note]    balances               networks
transfer <from> <to> <amount> [note]  log                    whoami
putfile <path> [account]              get <index>            sync
files                                 loss <0..1>            rebuild
getfile <index> <out-path>            robot start|stop       quit
```

## Binary files as peer-to-peer data

Rather than bolting on a file server, a file is injected into the *same*
replicated, totally-ordered log as money: a manifest item (name, size, SHA-256,
chunk count) followed by 30 KB base64 chunks.

```
alice> putfile ./statement.pdf alice
bob>   files                          # name, size, [complete]
bob>   getfile 0 ./copy.pdf           # reassembles, verifies SHA-256, writes
```

Because the chunks are ordinary ordered items, everything else is inherited for
free: they replicate to every node, a node joining later receives them through
state-sync, and dropped chunks are recovered by the same NACK path as
transactions. The hash is checked before anything is written, so a missing or
corrupt piece is detected rather than silently accepted.

## External HTTP interface

Start a node with `--api-port 8080` and anything outside the network — a script,
a service, curl — can post transactions in. They enter through the normal
ordered path, so they are sequenced and replicated exactly like console input.

```bash
curl -X POST localhost:8080/txn -d '{"kind":"deposit","account":"alice","amount":100}'
curl localhost:8080/balances
curl localhost:8080/log
```

## How it works

| Module | Responsibility |
|---|---|
| `davebank/protocol.py` | JSON-over-UDP wire format; strict validation, malformed packets dropped |
| `davebank/ledger.py` | Append-only ordered log; balances derived from it; file reassembly |
| `davebank/node.py` | The middleware: sockets, discovery, membership, election, ordering, NACK recovery, state sync |
| `davebank/external.py` | Optional HTTP interface |
| `davebank/__main__.py` | Console and the example-data robot |

Nine message types carry everything: `HELLO` / `WELCOME` for discovery,
`HEARTBEAT` for liveness (plus log height and peer roster), `GOODBYE` for
graceful departure, `SUBMIT` / `ORDERED` for the ordering path,
`STATE_REQUEST` / `STATE_RESPONSE` for joining, and `NACK` for recovery.

Each node runs four threads — a main-socket receiver, a broadcast-discovery
receiver, a maintenance loop (heartbeats, peer pruning, gap recovery) and the
console — with shared state behind locks.

### Design trade-offs

* **Full replication rather than sharding.** Every node answers every query from
  a complete local copy. At this scale that is more resilient than a Chord-style
  ring, where losing one shard owner loses data; at ten thousand nodes the
  answer flips.
* **A sequencer rather than quorum consensus.** One round trip instead of a
  majority round trip per transaction, at the cost of a brief changeover window
  during re-election — bounded by the fact that a new sequencer resumes from the
  highest sequence seen, and the ledger only ever applies the next expected
  index.
* **In-memory ledger.** State survives any node failing, because survivors hold
  it and a restarting node rebuilds from them — but not the whole network
  stopping at once.
* **No authentication.** Anyone on the LAN could inject a transaction. Signing
  items with per-node keys would be the natural next step; the ordered-log
  design would carry signatures without structural change.

## Tests

```bash
python tests/integration_smoke.py
```

Runs three real UDP nodes in one process and asserts the properties that matter:
membership convergence, total ordering across transactions submitted from
different nodes, full-ledger sync on join, sequencer failover, and NACK recovery
under simulated packet loss.

Two further harnesses drive a built single-file executable as separate OS
processes on localhost — closer to the real deployment:

```bash
pip install pyinstaller
pyinstaller --onefile --name project davebank_main.py   # -> dist/project.exe

python tests/local_demo.py    dist/project.exe   # core walkthrough
python tests/advanced_demo.py dist/project.exe   # 17 assertions over the advanced features
```

## Layout

```
davebank/            the system
  protocol.py        wire format and validation
  ledger.py          ordered log and balances
  node.py            the middleware
  external.py        HTTP interface
  __main__.py        console and robot
davebank_main.py     entry point for the single-file build
tests/               automated harnesses
report/REPORT.pdf    design write-up
```

## Note

Built as coursework for CSC4010 at Queen's University Belfast, and published as
a portfolio piece — please don't submit any part of it as your own work.
