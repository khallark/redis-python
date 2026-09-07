"""Stage 6 - SET & GET

Run from the repo root with the server already up:

    python3 stage_testers/6.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import NULL, OK, Client, array, bulk, main


async def test_set_get():
    """The stage requirement: SET returns +OK, GET returns the value."""
    c = await Client("c1").connect()
    await c.cmd("SET", "foo", "bar")
    await c.expect(OK)
    await c.cmd("GET", "foo")
    await c.expect(bulk("bar"))

    await c.cmd("SET", "baz", "qux")
    await c.expect(OK)
    await c.cmd("GET", "baz")
    await c.expect(bulk("qux"))

    # the first key must still be there
    await c.cmd("GET", "foo")
    await c.expect(bulk("bar"))
    await c.close()


async def test_missing_key():
    """A GET on an unset key is the null bulk string, not an empty one."""
    c = await Client("miss").connect()
    await c.cmd("GET", "definitely-not-set")
    await c.expect(NULL)
    await c.close()


async def test_overwrite():
    """SET on an existing key replaces the value."""
    c = await Client("over").connect()
    await c.cmd("SET", "k", "first")
    await c.expect(OK)
    await c.cmd("SET", "k", "second")
    await c.expect(OK)
    await c.cmd("GET", "k")
    await c.expect(bulk("second"))
    await c.close()


async def test_shared_keyspace():
    """A key SET by one connection must be visible to another.

    This is the local-vs-module-level dict check. A per-connection store
    passes every other scenario here and fails this one.
    """
    a = await Client("writer").connect()
    await a.cmd("SET", "shared", "yes")
    await a.expect(OK)
    await a.close()

    b = await Client("reader").connect()
    await b.cmd("GET", "shared")
    await b.expect(bulk("yes"))
    await b.close()


async def test_awkward_values():
    """Values are opaque bytes: RESP metacharacters must survive intact."""
    cases = {
        "empty": "",
        "dollar": "$5",
        "star": "*2",
        "crlf": "a\r\nb",
        "spaces": "hello world  ",
        "long": "x" * 500,
    }
    c = await Client("values").connect()
    for key, value in cases.items():
        await c.cmd("SET", key, value)
        await c.expect(OK)
    for key, value in cases.items():
        await c.cmd("GET", key)
        await c.expect(bulk(value))
    await c.close()


async def test_case_rules():
    """Command names are case-insensitive; keys and values are not."""
    c = await Client("case").connect()
    await c.cmd("set", "CaseKey", "CaseValue")
    await c.expect(OK)
    await c.cmd("GeT", "CaseKey")
    await c.expect(bulk("CaseValue"))

    # different casing is a different key
    await c.cmd("GET", "casekey")
    await c.expect(NULL)
    await c.close()


async def test_arity():
    """Missing arguments must produce an error, not a dropped connection."""
    c = await Client("arity").connect()
    await c.cmd("SET")
    await c.expect_error()
    await c.cmd("SET", "only-a-key")
    await c.expect_error()
    await c.cmd("GET")
    await c.expect_error()

    # the connection must still be usable afterwards
    await c.cmd("SET", "after", "errors")
    await c.expect(OK)
    await c.cmd("GET", "after")
    await c.expect(bulk("errors"))
    await c.close()


async def test_pipelined():
    """SET and GET in one packet must produce two replies in order."""
    c = await Client("pipe").connect()
    await c.send(array("SET", "p", "v") + array("GET", "p"))
    await c.expect(OK)
    await c.expect(bulk("v"))
    await c.close()


async def test_concurrent():
    """Interleaved writes from several clients must not cross wires."""
    async def one(n: int):
        c = await Client(f"burst-{n}").connect()
        await c.cmd("SET", f"key-{n}", f"val-{n}")
        await c.expect(OK)
        await c.cmd("GET", f"key-{n}")
        await c.expect(bulk(f"val-{n}"))
        await c.close()

    await asyncio.gather(*(one(i) for i in range(1, 5)))


SCENARIOS = [
    test_set_get,
    test_missing_key,
    test_overwrite,
    test_shared_keyspace,
    test_awkward_values,
    test_case_rules,
    test_arity,
    test_pipelined,
    test_concurrent,
]

if __name__ == "__main__":
    main(SCENARIOS)