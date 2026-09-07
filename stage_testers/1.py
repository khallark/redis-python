"""Stage 1 - Bind to a port

Run from the repo root with the server already up:

    python3 stage_testers/1.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import Client, main


async def test_accepts_connection():
    """The server listens on 6379 and accepts a TCP connection."""
    c = await Client("c1").connect()
    await c.close()


main([test_accepts_connection])