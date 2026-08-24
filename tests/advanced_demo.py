"""Drive the real project.exe through the ADVANCED demo steps and assert them.

`integration_smoke.py` covers the core properties in-process (ordering, join,
failover, NACK). This one drives the real executable through the rest:

  * `loss` unreliable-link simulation + automatic NACK recovery
  * a new node joining mid-session and receiving the whole ledger
  * binary files as P2P data items (putfile / files / getfile)
  * external HTTP interface (--api-port) posting into the network
  * logical network discovery (--network / `networks`)

Every step is checked programmatically instead of by eye: the script parses each
node's console output, prints PASS/FAIL per step, and exits non-zero if anything
failed. Nodes run on localhost, so this exercises everything except the physical
cross-machine firewall.

Run:  python tests/advanced_demo.py [path-to-project.exe]
"""

import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_EXE = os.path.join(ROOT, "dist", "project.exe")
EXE = sys.argv[1] if len(sys.argv) > 1 else _DEFAULT_EXE

HOST = "127.0.0.1"
SEED_PORT = 50000
API_PORT = 8080
WORKDIR = os.path.join(os.environ.get("TEMP", "."), "davebank_advanced_demo")

_PROMPT = re.compile(r"^(?:[A-Za-z0-9_\-]+>\s*)+")
_BALANCE = re.compile(r"^([A-Za-z_][\w\-]*)\s+(-?\d+\.\d\d)$")
_PEER = re.compile(r"^-\s+(\S+)\s+([0-9a-f]{6,8})\s+@\s+(\S+?)(\s+\*seq)?$")
_FILE = re.compile(r"^#(\d+)\s+(\S+)\s+(\d+) bytes\s+\[([^\]]+)\]")
_NETWORK = re.compile(r"^-\s+(\S+)\s+(\d+) node")
_LOGSIZE = re.compile(r"^log size\s*:\s*(\d+)")

results = []


class Node:
    """One project.exe process, driven through its stdin/stdout console."""

    def __init__(self, nick, args, host=HOST):
        self.nick = nick
        self.lines = []
        env = dict(os.environ, PYTHONUNBUFFERED="1")
        self.p = subprocess.Popen(
            [EXE, "--nick", nick, "--host", host] + args,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, bufsize=1, env=env,
        )
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

    def ask(self, cmd, wait=1.2):
        """Send a command and return the console lines it produced."""
        start = len(self.lines)
        self.send(cmd)
        time.sleep(wait)
        return [_PROMPT.sub("", ln).strip() for ln in self.lines[start:]]

    def transcript(self):
        return "\n".join(self.lines)

    def stop(self):
        self.send("quit")


# --------------------------------------------------------------------------- #
# parsing helpers
# --------------------------------------------------------------------------- #
def balances(node, wait=1.2):
    out = {}
    for line in node.ask("balances", wait):
        m = _BALANCE.match(line)
        if m:
            out[m.group(1)] = float(m.group(2))
    return out


def peer_nicks(node, wait=1.2):
    return [m.group(1) for m in
            (_PEER.match(ln) for ln in node.ask("peers", wait)) if m]


def log_size(node, wait=1.2):
    for line in node.ask("whoami", wait):
        m = _LOGSIZE.match(line)
        if m:
            return int(m.group(1))
    return -1


def files_listed(node, wait=1.5):
    out = []
    for line in node.ask("files", wait):
        m = _FILE.match(line)
        if m:
            out.append({"seq": int(m.group(1)), "name": m.group(2),
                        "size": int(m.group(3)), "state": m.group(4)})
    return out


def networks_listed(node, wait=1.5):
    return [(m.group(1), int(m.group(2))) for m in
            (_NETWORK.match(ln) for ln in node.ask("networks", wait)) if m]


def wait_until(fn, timeout=20.0, tick=1.0):
    """Poll fn() until it returns a truthy value or the timeout expires."""
    deadline = time.time() + timeout
    value = None
    while time.time() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(tick)
    return value


def check(step, ok, detail=""):
    results.append((step, bool(ok), detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {step}" + (f" -- {detail}" if detail else ""))
    return bool(ok)


def banner(t):
    print("\n" + "=" * 72 + f"\n{t}\n" + "=" * 72)


# --------------------------------------------------------------------------- #
def main():
    if not os.path.exists(EXE):
        print(f"exe not found: {EXE}")
        sys.exit(1)
    os.makedirs(WORKDIR, exist_ok=True)
    print(f"Using executable: {EXE}")
    print(f"Scratch dir     : {WORKDIR}")

    nodes = []
    try:
        # ------------------------------------------------------------------ #
        banner("SETUP: alice (seed, +HTTP API), bob, carol")
        alice = Node("alice", ["--port", str(SEED_PORT), "--api-port", str(API_PORT)])
        time.sleep(1.5)
        bob = Node("bob", ["--port", "50001", "--peer", f"{HOST}:{SEED_PORT}"])
        carol = Node("carol", ["--port", "50002", "--peer", f"{HOST}:{SEED_PORT}"])
        nodes = [alice, bob, carol]
        core = wait_until(lambda: all(len(peer_nicks(n, 0.4)) == 2 for n in nodes), 25)
        check("mesh formed (each node sees the other two)", core,
              f"alice={peer_nicks(alice, 0.4)} bob={peer_nicks(bob, 0.4)} "
              f"carol={peer_nicks(carol, 0.4)}")

        # ------------------------------------------------------------------ #
        banner("STEP 6: unreliable-link simulation (`loss`) + NACK recovery")
        print("  carol> loss 0.6   (drop 60% of carol's packets)")
        carol.ask("loss 0.6", 0.6)
        for i in range(5):
            bob.send(f"deposit lossy 20 lossy-{i}")
            time.sleep(0.3)
        time.sleep(3)
        lagging = balances(carol, 1.0).get("lossy", 0.0)
        print(f"  carol during 60% loss: lossy = {lagging:g} (bob sent 100)")

        print("  carol> loss 0     (link healthy again -> NACK recovery)")
        carol.ask("loss 0", 0.6)
        recovered = wait_until(
            lambda: balances(carol, 0.6).get("lossy") == 100.0, 25, 1.5)
        b_all = {n.nick: balances(n, 1.0) for n in nodes}
        check("carol recovered the missing items after loss cleared", recovered,
              f"carol lossy={b_all['carol'].get('lossy')}")
        check("all three nodes agree on balances after the lossy period",
              b_all["alice"] == b_all["bob"] == b_all["carol"],
              f"{b_all['alice']} | {b_all['bob']} | {b_all['carol']}")

        # ------------------------------------------------------------------ #
        banner("STEP 7: a new node joins mid-session and gets the whole ledger")
        alice_size = log_size(alice)
        dave = Node("dave", ["--port", "50003", "--peer", f"{HOST}:{SEED_PORT}"])
        nodes.append(dave)
        synced = wait_until(lambda: log_size(dave, 0.6) == alice_size, 20, 1.5)
        dave_bal = balances(dave, 1.2)
        check("dave synced the full ledger on join", synced,
              f"dave log={log_size(dave, 0.6)} vs alice log={alice_size}")
        check("dave's balances match the existing nodes",
              dave_bal == b_all["alice"], f"{dave_bal}")

        # ------------------------------------------------------------------ #
        banner("STEP 9: binary file as P2P data items (putfile / files / getfile)")
        src = os.path.join(WORKDIR, "statement.bin")
        blob = os.urandom(120_000)          # ~120 KB -> 4 chunks of 30 KB
        with open(src, "wb") as fh:
            fh.write(blob)
        src_sha = hashlib.sha256(blob).hexdigest()
        print(f"  test file: {src} ({len(blob)} bytes, sha256 {src_sha[:12]}...)")

        alice.ask(f"putfile {src} alice", 1.0)
        listed = wait_until(
            lambda: [f for f in files_listed(bob, 0.8)
                     if f["name"] == "statement.bin" and f["state"] == "complete"],
            25, 1.5)
        check("bob sees the uploaded file as [complete]", listed,
              str(listed or files_listed(bob, 0.8)))

        if listed:
            seq = listed[0]["seq"]
            check("file size replicated correctly",
                  listed[0]["size"] == len(blob),
                  f"{listed[0]['size']} bytes")
            out = os.path.join(WORKDIR, "bob_copy.bin")
            got = bob.ask(f"getfile {seq} {out}", 2.5)
            verified = any("sha256 verified OK" in ln for ln in got)
            check("bob's getfile reports sha256 verified OK", verified,
                  next((ln for ln in got if "wrote" in ln or "error" in ln), ""))
            same = (os.path.exists(out) and
                    hashlib.sha256(open(out, "rb").read()).hexdigest() == src_sha)
            check("retrieved bytes are identical to the original file", same)

            # A node that joined AFTER the upload must also get the file.
            late = Node("erin", ["--port", "50004", "--peer", f"{HOST}:{SEED_PORT}"])
            nodes.append(late)
            late_ok = wait_until(
                lambda: [f for f in files_listed(late, 0.8)
                         if f["name"] == "statement.bin" and f["state"] == "complete"],
                25, 1.5)
            check("a late-joining node receives the file via state sync", late_ok)

        # ------------------------------------------------------------------ #
        banner("STEP 10: external HTTP interface (--api-port)")
        posted = http_post(f"http://127.0.0.1:{API_PORT}/txn",
                           {"kind": "deposit", "account": "external", "amount": 500})
        check("POST /txn accepted by alice's HTTP API",
              isinstance(posted, dict) and posted.get("ok"), json.dumps(posted))
        seen = wait_until(
            lambda: balances(carol, 0.6).get("external") == 500.0, 15, 1.5)
        check("the external transaction reached carol over UDP", seen,
              f"carol external={balances(carol, 0.6).get('external')}")
        api_bal = http_get(f"http://127.0.0.1:{API_PORT}/balances")
        check("GET /balances matches the ledger",
              isinstance(api_bal, dict) and
              round(api_bal.get("external", 0), 2) == 500.0,
              json.dumps(api_bal))
        bad = http_post(f"http://127.0.0.1:{API_PORT}/txn", {"kind": "deposit"})
        check("malformed external POST rejected without killing the node",
              isinstance(bad, dict) and bad.get("ok") is False, json.dumps(bad))

        # ------------------------------------------------------------------ #
        banner("STEP 11: logical network discovery (--network / `networks`)")
        eve = Node("eve", ["--port", "50010", "--network", "testnet"], host="0.0.0.0")
        nodes.append(eve)
        found = wait_until(
            lambda: [n for n in networks_listed(alice, 0.8) if n[0] == "testnet"],
            12, 1.5)
        via = "broadcast discovery"
        if not found:
            # Broadcast can be filtered on a single host; fall back to the
            # unicast seed path, which is what the lab demo uses anyway.
            print("  broadcast did not surface testnet; retrying with a seeded node")
            eve2 = Node("eve2", ["--port", "50011", "--network", "testnet",
                                 "--peer", f"{HOST}:{SEED_PORT}"])
            nodes.append(eve2)
            found = wait_until(
                lambda: [n for n in networks_listed(alice, 0.8) if n[0] == "testnet"],
                15, 1.5)
            via = "unicast seed (--peer)"
        check("alice discovers the separate 'testnet' network", found,
              f"via {via}: {networks_listed(alice, 0.8)}")
        nicks = peer_nicks(alice, 1.0)
        check("networks stay isolated (eve is NOT one of alice's peers)",
              not any(n.startswith("eve") for n in nicks), f"peers={nicks}")

        # ------------------------------------------------------------------ #
        banner("FINAL: every node in the main network still agrees")
        main_nodes = [n for n in nodes if not n.nick.startswith("eve")]
        final = {n.nick: balances(n, 1.2) for n in main_nodes}
        agree = all(v == final[main_nodes[0].nick] for v in final.values())
        check("all main-network nodes hold identical balances", agree,
              " | ".join(f"{k}={v}" for k, v in final.items()))

    finally:
        banner("shutting down")
        for n in nodes:
            n.stop()
        time.sleep(2)
        for n in nodes:
            try:
                n.p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                n.p.terminate()

    banner("RESULT")
    failed = [s for s, ok, _ in results if not ok]
    for step, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {step}")
    if failed:
        print(f"\n  {len(failed)} of {len(results)} checks FAILED")
        if os.environ.get("DAVEBANK_DEMO_TRANSCRIPTS"):
            for n in nodes:
                banner(f"TRANSCRIPT: {n.nick}")
                print(n.transcript())
        sys.exit(1)
    print(f"\n  ALL {len(results)} ADVANCED CHECKS PASSED")


def http_post(url, obj):
    body = json.dumps(obj).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:          # 400 = rejected, still a reply
        return json.loads(e.read())
    except OSError as e:
        return {"ok": False, "error": f"connection failed: {e}"}


def http_get(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.loads(r.read())
    except OSError as e:
        return {"error": f"connection failed: {e}"}


if __name__ == "__main__":
    main()
