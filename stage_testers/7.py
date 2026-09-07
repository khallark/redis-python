"""Stage 7 - Expiry

Run from the repo root with the server already up:

    python3 stage_testers/7.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import NULL, OK, Client, bulk, main


async def test_px_expiry():
    """The stage requirement: SET with PX, readable now, gone after the TTL."""
    c = await Client("px").connect()
    await c.cmd("SET", "foo", "bar", "PX", "100")
    await c.expect(OK)
    await c.cmd("GET", "foo")
    await c.expect(bulk("bar"))

    await asyncio.sleep(0.2)
    await c.cmd("GET", "foo")
    await c.expect(NULL)
    await c.close()


async def test_no_expiry_persists():
    """A SET without an expiry option never expires."""
    c = await Client("noexp").connect()
    await c.cmd("SET", "permanent", "value")
    await c.expect(OK)
    await asyncio.sleep(0.2)
    await c.cmd("GET", "permanent")
    await c.expect(bulk("value"))
    await c.close()


async def test_set_clears_previous_ttl():
    """Re-SETting a key without an option drops the old deadline.

    Catches a stale entry left in the expires dict: the new value inherits
    the previous key's TTL and vanishes when it should not.
    """
    c = await Client("clear").connect()
    await c.cmd("SET", "k", "first", "PX", "100")
    await c.expect(OK)
    await c.cmd("SET", "k", "second")
    await c.expect(OK)

    await asyncio.sleep(0.2)
    await c.cmd("GET", "k")
    await c.expect(bulk("second"))
    await c.close()


async def test_set_replaces_ttl():
    """A new expiry option overrides the previous one."""
    c = await Client("replace").connect()
    await c.cmd("SET", "r", "v", "PX", "50")
    await c.expect(OK)
    await c.cmd("SET", "r", "v", "PX", "400")
    await c.expect(OK)

    await asyncio.sleep(0.2)
    await c.cmd("GET", "r")
    await c.expect(bulk("v"))
    await c.close()


async def test_lowercase_option():
    """Option names are case-insensitive."""
    c = await Client("lower").connect()
    await c.cmd("set", "low", "val", "px", "100")
    await c.expect(OK)
    await c.cmd("GET", "low")
    await c.expect(bulk("val"))

    await asyncio.sleep(0.2)
    await c.cmd("GET", "low")
    await c.expect(NULL)
    await c.close()


async def test_ex_seconds():
    """EX is seconds, not milliseconds.

    Treating EX 10 as 10ms would make the key dead almost immediately.
    """
    c = await Client("ex").connect()
    await c.cmd("SET", "e", "v", "EX", "10")
    await c.expect(OK)
    await asyncio.sleep(0.2)
    await c.cmd("GET", "e")
    await c.expect(bulk("v"))
    await c.close()


async def test_expired_is_null_not_empty():
    """An expired key is the null bulk string, not the empty string."""
    c = await Client("null").connect()
    await c.cmd("SET", "gone", "x", "PX", "50")
    await c.expect(OK)
    await asyncio.sleep(0.2)
    await c.cmd("GET", "gone")
    await c.expect(NULL)          # exactly $-1\r\n, never $0\r\n\r\n
    await c.close()


async def test_bad_options():
    """Malformed options error out and leave the connection usable."""
    c = await Client("bad").connect()

    await c.cmd("SET", "k", "v", "PX")            # option with no value
    await c.expect_error()
    await c.cmd("SET", "k", "v", "PX", "abc")     # non-integer value
    await c.expect_error()
    await c.cmd("SET", "k", "v", "BOGUS", "1")    # unknown option
    await c.expect_error()

    await c.cmd("SET", "after", "errors")
    await c.expect(OK)
    await c.cmd("GET", "after")
    await c.expect(bulk("errors"))
    await c.close()


async def test_expiry_across_connections():
    """A TTL belongs to the keyspace, not to the connection that set it."""
    a = await Client("setter").connect()
    await a.cmd("SET", "x", "v", "PX", "100")
    await a.expect(OK)
    await a.close()

    await asyncio.sleep(0.2)
    b = await Client("getter").connect()
    await b.cmd("GET", "x")
    await b.expect(NULL)
    await b.close()


SCENARIOS = [
    test_px_expiry,
    test_no_expiry_persists,
    test_set_clears_previous_ttl,
    test_set_replaces_ttl,
    test_lowercase_option,
    test_ex_seconds,
    test_expired_is_null_not_empty,
    test_bad_options,
    test_expiry_across_connections,
]

if __name__ == "__main__":
    main(SCENARIOS)