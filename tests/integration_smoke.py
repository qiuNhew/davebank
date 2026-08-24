"""Integration smoke test: spins up real UDP nodes in one process and checks
total ordering, join-sync and sequencer failover. Not part of the submission
package -- a developer harness. Run: python tests/integration_smoke.py
"""
import sys, time, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from davebank.node import Node

BASE = 50600
DISC = 51000  # give each node a distinct discovery port -> deterministic (seed-based)

def make(nick, port, seed=None, disc=0):
    return Node(nickname=nick, host="127.0.0.1", port=port,
                discovery_port=DISC + disc, seed=seed, on_event=lambda m: None)

def ledger_fingerprint(node):
    return tuple((node.ledger.get(s).item_id, node.ledger.get(s).kind,
                  round(node.ledger.get(s).amount, 2)) for s in range(node.ledger.size()))

def wait_converge(nodes, expected_size, timeout=15.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        sizes = [n.ledger.size() for n in nodes]
        if all(s == expected_size for s in sizes):
            fps = {ledger_fingerprint(n) for n in nodes}
            if len(fps) == 1:
                return True
        time.sleep(0.3)
    return False

def main():
    failures = []
    alice = make("alice", BASE + 0, disc=0)
    bob   = make("bob",   BASE + 1, seed=("127.0.0.1", BASE + 0), disc=1)
    carol = make("carol", BASE + 2, seed=("127.0.0.1", BASE + 0), disc=2)
    for n in (alice, bob, carol):
        n.start()

    print("waiting for membership convergence...")
    time.sleep(4)
    for n in (alice, bob, carol):
        print(f"  {n.nickname}: knows {len(n.peers_view())} peers, "
              f"sequencer={'me' if n.is_sequencer() else n._sequencer_id()[:8]}")
    if not all(len(n.peers_view()) == 2 for n in (alice, bob, carol)):
        failures.append("membership did not converge to 3 nodes")

    # --- total ordering: submit from different nodes concurrently ---
    print("submitting transactions from all three nodes...")
    alice.submit("deposit", "alice", 100)
    bob.submit("deposit", "bob", 50)
    carol.submit("transfer", "alice", 30, target="carol")
    bob.submit("withdraw", "bob", 20)
    alice.submit("transfer", "carol", 10, target="bob")

    if wait_converge([alice, bob, carol], 5):
        print("  OK: all 3 ledgers identical, 5 items in the same order")
        print(f"  balances @ carol: {carol.ledger.all_balances()}")
        # alice: +100 -30 = 70 ; bob: +50 -20 +10 = 40 ; carol: +30 -10 = 20
        b = carol.ledger.all_balances()
        if not (abs(b.get('alice',0)-70)<1e-6 and abs(b.get('bob',0)-40)<1e-6 and abs(b.get('carol',0)-20)<1e-6):
            failures.append(f"balances wrong: {b}")
    else:
        failures.append("ledgers did not converge after submits")
        for n in (alice, bob, carol):
            print(f"    {n.nickname} size={n.ledger.size()} fp={ledger_fingerprint(n)}")

    # --- join sync: a new node must receive the whole ledger ---
    print("joining a fresh node 'dave'...")
    dave = make("dave", BASE + 3, seed=("127.0.0.1", BASE + 0), disc=3)
    dave.start()
    if wait_converge([alice, dave], 5):
        print("  OK: dave synced the full 5-item ledger on join")
    else:
        failures.append(f"dave failed to sync (size={dave.ledger.size()})")

    # --- sequencer failover: kill the current sequencer, keep transacting ---
    nodes = [alice, bob, carol, dave]
    seq_id = alice._sequencer_id()
    seq_node = next(n for n in nodes if n.node_id == seq_id)
    survivors = [n for n in nodes if n is not seq_node]
    print(f"killing sequencer '{seq_node.nickname}' and waiting for re-election...")
    seq_node.stop()
    for i in range(5):  # allow heartbeat-timeout pruning + re-election
        time.sleep(2)
        print("    t+%ds " % (2*(i+1)) + " | ".join(
            f"{n.nickname}:peers={len(n.peers_view())},seq={'me' if n.is_sequencer() else n._sequencer_id()[:6]}"
            for n in survivors))
    new_seq = survivors[0]._sequencer_id()
    print(f"  new sequencer = {next(n.nickname for n in survivors if n.node_id==new_seq)}")
    if new_seq == seq_id:
        failures.append("sequencer was not re-elected after failure")
    # instrument survivors to trace recovery
    for n in survivors:
        n._on_event = (lambda nn: (lambda m: print(f"    [{nn.nickname}] {m}")))(n)
    survivors[0].submit("deposit", "recovery", 999)
    print(f"  submitted #5 via {survivors[0].nickname}; "
          f"its peers now: {[p[1] for p in survivors[0].peers_view()]}")
    if wait_converge(survivors, 6):
        print("  OK: survivors kept working after failover (6th item applied everywhere)")
    else:
        failures.append("survivors did not converge after failover")
        for n in survivors:
            print(f"    {n.nickname} size={n.ledger.size()}")

    for n in survivors:
        n.stop()

    print("\n=== RESULT ===")
    if failures:
        for f in failures:
            print("  FAIL:", f)
        sys.exit(1)
    print("  ALL CHECKS PASSED")

if __name__ == "__main__":
    main()
