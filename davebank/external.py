"""External interface for DaveBank (a "could have" feature).

Exposes a tiny HTTP API on a separate port so that programs *outside* the
peer-to-peer network - a shell script, another service, a web page - can post
transactions into the network and read back the current state. Anything posted
here is injected through the normal `Node.submit()` path, so it is totally
ordered and replicated exactly like transactions typed at a node's console.

Endpoints:
    POST /txn        body: {"kind","account","amount"[,"target","note"]}
    GET  /balances   -> {account: balance, ...}
    GET  /log        -> [ {data item}, ... ]
    GET  /           -> service description

Uses only the standard library (http.server); no framework. Enable with
`--api-port N` on the command line.

Example (PowerShell):
    Invoke-RestMethod -Method Post http://localhost:8080/txn `
        -Body '{"kind":"deposit","account":"alice","amount":100}'
Example (curl):
    curl -X POST localhost:8080/txn -d '{"kind":"deposit","account":"a","amount":5}'
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class _Handler(BaseHTTPRequestHandler):
    # Silence the default per-request logging to stderr.
    def log_message(self, *args):
        pass

    def _reply(self, code: int, obj) -> None:
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        node = self.server.node
        path = self.path.split("?", 1)[0]
        if path == "/balances":
            self._reply(200, node.ledger.all_balances())
        elif path == "/log":
            self._reply(200, node.ledger.snapshot())
        else:
            self._reply(200, {
                "service": "DaveBank external API",
                "node": node.nickname,
                "network": node.network,
                "endpoints": ["POST /txn", "GET /balances", "GET /log"],
            })

    def do_POST(self):
        node = self.server.node
        if self.path.split("?", 1)[0] != "/txn":
            self._reply(404, {"ok": False, "error": "unknown endpoint"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length) or b"{}")
            item_id = node.submit(
                kind=payload["kind"],
                account=payload["account"],
                amount=float(payload["amount"]),
                target=payload.get("target"),
                note=str(payload.get("note", "external")),
            )
            self._reply(200, {"ok": True, "item_id": item_id})
        except (KeyError, ValueError, TypeError, json.JSONDecodeError) as e:
            # Malformed external data is rejected, not fatal.
            self._reply(400, {"ok": False, "error": str(e)})


class ExternalAPI:
    """Runs the HTTP interface in a background thread."""

    def __init__(self, node, port: int, host: str = "0.0.0.0") -> None:
        self.node = node
        self.port = port
        self.host = host
        self._srv = None
        self._thread = None

    def start(self) -> None:
        self._srv = ThreadingHTTPServer((self.host, self.port), _Handler)
        self._srv.node = self.node
        self._thread = threading.Thread(target=self._srv.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._srv is not None:
            try:
                self._srv.shutdown()
                self._srv.server_close()
            except Exception:
                pass
