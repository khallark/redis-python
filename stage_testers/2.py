"""Stage 2 - Respond to PING

Run from the repo root with the server already up:

    python3 stage_testers/2.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import PONG, Client, main


async def test_ping():
    """A single PING gets a single +PONG."""
    c = await Client("c1").connect()
    await c.cmd("PING")
    await c.expect(PONG)
    await c.close()


main([test_ping])