"""Stage 3 - Respond to multiple PINGs

Run from the repo root with the server already up:

    python3 stage_testers/3.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import PONG, Client, main


async def test_multiple_pings():
    """Several PINGs on one connection get one +PONG each."""
    c = await Client("c1").connect()
    for _ in range(4):
        await c.cmd("PING")
        await c.expect(PONG)
    await c.close()


main([test_multiple_pings])