"""Stage 5 - ECHO (and a replay of stages 1-4)

Run from the repo root with the server already up:

    python3 stage_testers/5.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import PONG, Client, array, bulk, main


async def test_echo():
    """The stage requirement: ECHO returns its argument as a bulk string."""
    c = await Client("echo").connect()
    await c.cmd("ECHO", "hey")
    await c.expect(bulk("hey"))
    await c.cmd("ECHO", "banana")
    await c.expect(bulk("banana"))
    await c.close()


async def test_awkward_values():
    """The argument is opaque: RESP metacharacters must survive intact."""
    c = await Client("values").connect()
    for value in ("", "$5", "*2", "a\r\nb", "hello world  ", "x" * 500):
        await c.cmd("ECHO", value)
        await c.expect(bulk(value))
    await c.close()


async def test_case_insensitive():
    """Command names are case-insensitive; the argument is not."""
    c = await Client("case").connect()
    await c.cmd("echo", "MiXeD")
    await c.expect(bulk("MiXeD"))
    await c.cmd("EcHo", "MiXeD")
    await c.expect(bulk("MiXeD"))
    await c.close()


async def test_arity():
    """Wrong argument counts must error, not drop the connection."""
    c = await Client("arity").connect()
    await c.cmd("ECHO")
    await c.expect_error()
    await c.cmd("ECHO", "a", "b")
    await c.expect_error()

    # the connection must still be usable afterwards
    await c.cmd("ECHO", "still-here")
    await c.expect(bulk("still-here"))
    await c.close()


async def test_unknown_command():
    """An unrecognised command gets an error reply rather than silence."""
    c = await Client("unknown").connect()
    await c.cmd("NOSUCHCOMMAND")
    await c.expect_error()
    await c.cmd("PING")
    await c.expect(PONG)
    await c.close()


async def test_interleaved():
    """Two open connections, alternating commands.

    A serial server passes a naive 'both clients connect and send once' test,
    because it can finish client A entirely before touching client B. Forcing
    B to get a reply while A is still open and idle is what actually proves
    concurrency.
    """
    a = await Client("client-a").connect()
    b = await Client("client-b").connect()

    await a.cmd("PING")
    await a.expect(PONG)

    await b.cmd("ECHO", "hey")
    await b.expect(bulk("hey"))

    await a.cmd("ECHO", "world")
    await a.expect(bulk("world"))

    await b.cmd("PING")
    await b.expect(PONG)

    # one client leaving must not disturb the other
    await b.close()
    await a.cmd("PING")
    await a.expect(PONG)
    await a.close()


async def test_simultaneous():
    """Several clients firing at once, to shake out shared-state bugs."""

    async def one(n: int):
        c = await Client(f"burst-{n}").connect()
        await c.cmd("ECHO", f"msg-{n}")
        await c.expect(bulk(f"msg-{n}"))
        await c.close()

    await asyncio.gather(*(one(i) for i in range(1, 5)))


async def test_multiple_pings():
    """Stage 3: many commands on one connection, one reply each."""
    c = await Client("pings").connect()
    for _ in range(5):
        await c.cmd("PING")
        await c.expect(PONG)
    await c.close()


async def test_pipelined():
    """Two commands in one packet must produce two replies."""
    c = await Client("pipe").connect()
    await c.send(array("PING") + array("ECHO", "hi"))
    await c.expect(PONG)
    await c.expect(bulk("hi"))
    await c.close()


async def test_split():
    """A command cut across two packets must be reassembled."""
    c = await Client("split").connect()
    payload = array("ECHO", "abc")
    await c.send(payload[:9])
    await asyncio.sleep(0.2)
    await c.send(payload[9:])
    await c.expect(bulk("abc"))
    await c.close()


async def test_split_then_pipelined():
    """One packet may finish a partial command and carry a whole next one."""
    c = await Client("mixed").connect()
    stream = array("ECHO", "hello") + array("PING")
    await c.send(stream[:21])
    await asyncio.sleep(0.2)
    await c.send(stream[21:])
    await c.expect(bulk("hello"))
    await c.expect(PONG)
    await c.close()


SCENARIOS = [
    test_echo,
    test_awkward_values,
    test_case_insensitive,
    test_arity,
    test_unknown_command,
    test_interleaved,
    test_simultaneous,
    test_multiple_pings,
    test_pipelined,
    test_split,
    test_split_then_pipelined,
]

if __name__ == "__main__":
    main(SCENARIOS)