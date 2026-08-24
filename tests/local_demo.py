"""Mimic the lab-bench demo on ONE machine using the real project.exe.

Launches several project.exe processes on localhost (different ports, joined via
--peer) and drives them through the demo script by writing to their stdin, then
prints each node's full transcript. This reproduces everything the lab demo
does EXCEPT the physical cross-machine firewall (localhost is not firewalled).

Run:  python tests/local_demo.py  [path-to-project.exe]
"""
import os
import subprocess
import sys
import threading
import time

_DEFAULT_EXE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dist", "project.exe")
EXE = sys.argv[1] if len(sys.argv) > 1 else _DEFAULT_EXE
SEED_PORT = 50000


class Node:
    def __init__(self, nick, args):
        self.nick = nick
        env = dict(os.environ, PYTHONUNBUFFERED="1")
        self.p = subprocess.Popen(
            [EXE] + args,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, bufsize=1, env=env,
        )
        self.lines = []
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        for line in self.p.stdout:
            self.lines.append(line.rstrip("\n"))

    def send(self, cmd):
        try:
            self.p.stdin.write(cmd + "\n")
            self.p.stdin.flush()
        except (BrokenPipeError, OSError):
            pass

    def transcript(self):
        return "\n".join(self.lines)


def banner(t):
    print("\n" + "=" * 70 + f"\n{t}\n" + "=" * 70)


def main():
    if not os.path.exists(EXE):
        print(f"exe not found: {EXE}")
        sys.exit(1)
    print(f"Using executable: {EXE}")
    print("Mimicking a 3-machine lab bench on localhost (ports 50000-50002).\n")

    banner("STEP 1-4: start seed node + join two more (like 3 lab machines)")
    alice = Node("alice", ["--nick", "alice", "--host", "127.0.0.1", "--port", str(SEED_PORT)])
    time.sleep(1.5)
    bob = Node("bob", ["--nick", "bob", "--host", "127.0.0.1", "--port", "50001",
                       "--peer", f"127.0.0.1:{SEED_PORT}"])
    carol = Node("carol", ["--nick", "carol", "--host", "127.0.0.1", "--port", "50002",
                           "--peer", f"127.0.0.1:{SEED_PORT}"])
    nodes = [alice, bob, carol]
    print("launched alice (seed), bob, carol; waiting for them to find each other...")
    time.sleep(4)

    banner("STEP 5: verify the network formed (peers on each node)")
    for n in nodes:
        n.send("peers")
    time.sleep(1.5)

    banner("STEP 6: submit transactions from DIFFERENT nodes (total ordering)")
    alice.send("deposit alice 100")
    bob.send("deposit bob 50")
    carol.send("transfer alice bob 30")
    time.sleep(2.5)

    banner("STEP 6b: every node should show IDENTICAL balances + index order")
    for n in nodes:
        n.send("balances")
        n.send("log")
    time.sleep(2)

    banner("STEP 6c: robot auto-data on bob for 3s, then stop")
    bob.send("robot start 0.5")
    time.sleep(3)
    bob.send("robot stop")
    time.sleep(1.5)
    for n in nodes:
        n.send("whoami")
    time.sleep(1.5)

    banner("STEP 6d: FAILOVER - kill the SEQUENCER, network must re-elect + keep working")
    # Find the current sequencer by asking each node its role.
    for n in nodes:
        n.send("whoami")
    time.sleep(1.5)

    def is_sequencer(n):
        # look at the most recent whoami block in the transcript
        text = "\n".join(n.lines)
        idx = text.rfind("role")
        return idx != -1 and "SEQUENCER" in text[idx:idx + 40]

    seq_node = next((n for n in nodes if is_sequencer(n)), alice)
    survivors = [n for n in nodes if n is not seq_node]
    print(f"current sequencer is '{seq_node.nick}' -> quitting it (graceful GOODBYE)")
    seq_node.send("quit")
    time.sleep(3)          # graceful GOODBYE -> near-instant re-election

    print(f"survivors: {[n.nick for n in survivors]} - checking new sequencer + submitting")
    for n in survivors:
        n.send("whoami")
    survivors[0].send("deposit recovery 999")
    time.sleep(2)
    for n in survivors:
        n.send("peers")
        n.send("balances")
    time.sleep(2)

    banner("shutting down")
    for n in survivors:
        n.send("quit")
    time.sleep(2)
    for n in nodes:
        try:
            n.p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            n.p.terminate()

    for n in nodes:
        banner(f"TRANSCRIPT: {n.nick}")
        print(n.transcript())


if __name__ == "__main__":
    main()
