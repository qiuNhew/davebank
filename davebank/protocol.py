"""Wire protocol for DaveBank.

All inter-node communication is hand-rolled on top of UDP datagrams. We use
JSON as the on-the-wire encoding because it is human-readable (great for a
demo) and trivial to validate. This module owns:

  * the set of message types exchanged between nodes,
  * encoding a message dict to bytes and decoding bytes back to a dict,
  * defensive validation so that malformed / hostile packets are rejected
    rather than crashing a node (a "Should have" requirement).

NOTE: we deliberately do not use any messaging middleware (ZeroMQ, RabbitMQ,
MPI, RMI, ...). Everything here is plain `socket` + `json` from the standard
library.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

# Maximum size of a single UDP payload we are willing to accept. Standard
# Ethernet MTU leaves us well under the theoretical 65507 byte UDP limit; we
# keep messages small and reject anything implausibly large to avoid abuse.
MAX_DATAGRAM = 60000

# ---------------------------------------------------------------------------
# Message types
# ---------------------------------------------------------------------------
# Discovery / membership
HELLO = "HELLO"            # broadcast or unicast: "I exist, here is who I am"
WELCOME = "WELCOME"        # unicast reply to HELLO carrying the known peer set
HEARTBEAT = "HEARTBEAT"    # periodic liveness signal
GOODBYE = "GOODBYE"        # graceful disconnect announcement

# Transaction ordering (total-order multicast)
SUBMIT = "SUBMIT"          # origin -> sequencer: please order this transaction
ORDERED = "ORDERED"        # sequencer -> all: transaction with a global seq no.

# State synchronisation / recovery
STATE_REQUEST = "STATE_REQUEST"    # joining node -> peer: send me the log
STATE_RESPONSE = "STATE_RESPONSE"  # peer -> joining node: here is the log
NACK = "NACK"              # node -> peer: I'm missing these sequence numbers

# Every legal message type, used for fast validation.
MESSAGE_TYPES = {
    HELLO, WELCOME, HEARTBEAT, GOODBYE,
    SUBMIT, ORDERED, STATE_REQUEST, STATE_RESPONSE, NACK,
}


def encode(message: Dict[str, Any]) -> bytes:
    """Serialise a message dict to UTF-8 JSON bytes ready for the socket."""
    return json.dumps(message, separators=(",", ":")).encode("utf-8")


def decode(data: bytes) -> Optional[Dict[str, Any]]:
    """Parse raw UDP bytes into a validated message dict.

    Returns ``None`` for anything malformed: oversized packets, non-JSON
    payloads, non-object JSON, an unknown/absent ``type`` field, or a missing
    sender id. Callers simply drop ``None`` results, which is how the system
    "handles malformed data gracefully".
    """
    if not data or len(data) > MAX_DATAGRAM:
        return None
    try:
        message = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(message, dict):
        return None
    if message.get("type") not in MESSAGE_TYPES:
        return None
    # Every message must identify its sender so we can track membership.
    if not isinstance(message.get("node_id"), str):
        return None
    return message
