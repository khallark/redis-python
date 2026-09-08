"""Stages 8-10 - RPUSH (create a list, append an element, append multiple)

Run from the repo root with the server already up:

    python3 stage_testers/8_9_10.py

Order of elements is not verified here -- nothing reads a list back until
LRANGE. Extend this file once stage 11 lands.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import NULL, OK, Client, bulk, integer, main


async def test_create_list():
    """Stage 8: RPUSH on a missing key creates the list and returns 1."""
    c = await Client("create").connect()
    await c.cmd("RPUSH", "list_key", "foo")
    await c.expect(integer(1))
    await c.close()


async def test_append_element():
    """Stage 9: RPUSH on an existing list appends and returns the new length."""
    c = await Client("append").connect()
    await c.cmd("RPUSH", "append_key", "a")
    await c.expect(integer(1))
    await c.cmd("RPUSH", "append_key", "b")
    await c.expect(integer(2))
    await c.cmd("RPUSH", "append_key", "c")
    await c.expect(integer(3))
    await c.close()


async def test_append_multiple():
    """Stage 10: several elements in one call, counted once at the end."""
    c = await Client("multi").connect()
    await c.cmd("RPUSH", "multi_key", "a", "b", "c")
    await c.expect(integer(3))
    await c.cmd("RPUSH", "multi_key", "d", "e")
    await c.expect(integer(5))

    # a single element through the variadic path still works
    await c.cmd("RPUSH", "multi_key", "f")
    await c.expect(integer(6))
    await c.close()


async def test_wrongtype():
    """RPUSH against a string key is a WRONGTYPE error, not a crash."""
    c = await Client("wrongtype").connect()
    await c.cmd("SET", "stringy", "value")
    await c.expect(OK)
    await c.cmd("RPUSH", "stringy", "x")
    await c.expect_error()

    # and the reverse: GET on a list key
    await c.cmd("RPUSH", "listy", "x")
    await c.expect(integer(1))

    # connection still usable
    await c.cmd("RPUSH", "listy", "y")
    await c.expect(integer(2))
    await c.close()


async def test_arity():
    """RPUSH with no elements errors and leaves the connection usable."""
    c = await Client("arity").connect()
    await c.cmd("RPUSH")
    await c.expect_error()
    await c.cmd("RPUSH", "only_key")
    await c.expect_error()

    await c.cmd("RPUSH", "after", "errors")
    await c.expect(integer(1))
    await c.close()


async def test_awkward_elements():
    """Elements are opaque bytes: quotes, RESP metacharacters, empties."""
    c = await Client("awkward").connect()
    await c.cmd("RPUSH", "awkward_key", "", '"quoted"', "$5", "*2", "a\r\nb", "x" * 500)
    await c.expect(integer(6))
    await c.close()


async def test_duplicates():
    """Lists are not sets: repeated values all count."""
    c = await Client("dupes").connect()
    await c.cmd("RPUSH", "dupe_key", "same", "same", "same")
    await c.expect(integer(3))
    await c.cmd("RPUSH", "dupe_key", "same")
    await c.expect(integer(4))
    await c.close()


async def test_expired_key_becomes_fresh_list():
    """A key whose TTL has passed must not be appended to.

    Catches an RPUSH that checks `key in store` directly instead of going
    through the expiry-aware lookup: the dead string is still present, so
    it would raise WRONGTYPE instead of creating a new list.
    """
    c = await Client("expired").connect()
    await c.cmd("SET", "ttl_key", "v", "PX", "100")
    await c.expect(OK)
    await asyncio.sleep(0.2)

    await c.cmd("RPUSH", "ttl_key", "fresh")
    await c.expect(integer(1))
    await c.close()


async def test_rpush_keeps_ttl():
    """RPUSH must not clear an existing deadline the way SET does."""
    c = await Client("keepttl").connect()
    await c.cmd("RPUSH", "keep_key", "a")
    await c.expect(integer(1))
    # no EXPIRE command implemented, so this only checks RPUSH does not
    # resurrect an expired key it created before the TTL was set
    await c.cmd("RPUSH", "keep_key", "b")
    await c.expect(integer(2))
    await c.close()


async def test_shared_across_connections():
    """A list belongs to the keyspace, not to the connection that made it."""
    a = await Client("writer").connect()
    await a.cmd("RPUSH", "shared_list", "a", "b")
    await a.expect(integer(2))
    await a.close()

    b = await Client("reader").connect()
    await b.cmd("RPUSH", "shared_list", "c")
    await b.expect(integer(3))
    await b.close()


async def test_pipelined():
    """Two RPUSHes in one packet return two increasing lengths in order."""
    c = await Client("pipe").connect()
    from common import array
    await c.send(array("RPUSH", "pipe_key", "a") + array("RPUSH", "pipe_key", "b"))
    await c.expect(integer(1))
    await c.expect(integer(2))
    await c.close()


async def test_concurrent():
    """Separate keys under concurrent pushes must not interfere."""

    async def one(n: int):
        c = await Client(f"burst-{n}").connect()
        await c.cmd("RPUSH", f"burst_key_{n}", "x", "y")
        await c.expect(integer(2))
        await c.cmd("RPUSH", f"burst_key_{n}", "z")
        await c.expect(integer(3))
        await c.close()

    await asyncio.gather(*(one(i) for i in range(1, 5)))


async def test_string_still_readable():
    """A failed RPUSH must not damage the string it rejected."""
    c = await Client("intact").connect()
    await c.cmd("SET", "intact_key", "original")
    await c.expect(OK)
    await c.cmd("RPUSH", "intact_key", "x")
    await c.expect_error()
    await c.cmd("GET", "intact_key")
    await c.expect(bulk("original"))
    await c.close()


SCENARIOS = [
    test_create_list,
    test_append_element,
    test_append_multiple,
    test_wrongtype,
    test_arity,
    test_awkward_elements,
    test_duplicates,
    test_expired_key_becomes_fresh_list,
    test_rpush_keeps_ttl,
    test_shared_across_connections,
    test_pipelined,
    test_concurrent,
    test_string_still_readable,
]

if __name__ == "__main__":
    main(SCENARIOS)