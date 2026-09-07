"""Stage 4 - Handle concurrent clients

Run from the repo root with the server already up:

    python3 stage_testers/4.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import PONG, Client, main


async def test_concurrent_clients():
    """Several connections stay open at once and are served independently."""
    a = await Client("client-a").connect()
    b = await Client("client-b").connect()
    c = await Client("client-c").connect()

    await a.cmd("PING")
    await a.expect(PONG)

    await b.cmd("PING")
    await b.expect(PONG)

    await a.cmd("PING")
    await a.expect(PONG)

    await c.cmd("PING")
    await c.expect(PONG)

    await b.cmd("PING")
    await b.expect(PONG)

    await a.close()
    await b.close()
    await c.close()


main([test_concurrent_clients])