"""Stage 13 - LPUSH

Run from the repo root with the server already up:

    python3 stage_testers/13.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import OK, Client, array, bulk, integer, main, resp_array


async def test_create_list():
    """LPUSH on a missing key creates the list and returns 1."""
    c = await Client("create").connect()
    await c.cmd("LPUSH", "lp_create_key", "foo")
    await c.expect(integer(1))
    await c.cmd("LRANGE", "lp_create_key", "0", "-1")
    await c.expect(resp_array("foo"))
    await c.close()


async def test_prepends():
    """Each LPUSH puts its element at the head, so order reverses."""
    c = await Client("prepend").connect()
    await c.cmd("LPUSH", "lp_prepend_key", "a")
    await c.expect(integer(1))
    await c.cmd("LPUSH", "lp_prepend_key", "b")
    await c.expect(integer(2))
    await c.cmd("LPUSH", "lp_prepend_key", "c")
    await c.expect(integer(3))

    await c.cmd("LRANGE", "lp_prepend_key", "0", "-1")
    await c.expect(resp_array("c", "b", "a"))      # last pushed is first
    await c.close()


async def test_multiple_elements_reverse():
    """LPUSH key a b c leaves the list as c, b, a.

    Each element is pushed to the head in argument order, so the first
    argument ends up deepest. Appending the batch without reversing it
    would give a, b, c and pass every length check.
    """
    c = await Client("multi").connect()
    await c.cmd("LPUSH", "lp_multi_key", "a", "b", "c")
    await c.expect(integer(3))
    await c.cmd("LRANGE", "lp_multi_key", "0", "-1")
    await c.expect(resp_array("c", "b", "a"))

    await c.cmd("LPUSH", "lp_multi_key", "d", "e")
    await c.expect(integer(5))
    await c.cmd("LRANGE", "lp_multi_key", "0", "-1")
    await c.expect(resp_array("e", "d", "c", "b", "a"))
    await c.close()


async def test_mixed_with_rpush():
    """LPUSH and RPUSH work on the same list from opposite ends."""
    c = await Client("mixed").connect()
    await c.cmd("RPUSH", "lp_mixed_key", "1")
    await c.expect(integer(1))
    await c.cmd("LPUSH", "lp_mixed_key", "0")
    await c.expect(integer(2))
    await c.cmd("RPUSH", "lp_mixed_key", "2")
    await c.expect(integer(3))
    await c.cmd("LPUSH", "lp_mixed_key", "-1")
    await c.expect(integer(4))

    await c.cmd("LRANGE", "lp_mixed_key", "0", "-1")
    await c.expect(resp_array("-1", "0", "1", "2"))
    await c.close()


async def test_negative_range_after_lpush():
    """Index arithmetic still holds once the head has moved."""
    c = await Client("indexes").connect()
    await c.cmd("LPUSH", "lp_idx_key", "a", "b", "c", "d")     # d c b a
    await c.expect(integer(4))

    await c.cmd("LRANGE", "lp_idx_key", "0", "0")
    await c.expect(resp_array("d"))
    await c.cmd("LRANGE", "lp_idx_key", "-1", "-1")
    await c.expect(resp_array("a"))
    await c.cmd("LRANGE", "lp_idx_key", "1", "2")
    await c.expect(resp_array("c", "b"))
    await c.close()


async def test_wrongtype():
    """LPUSH against a string key errors and leaves the string intact."""
    c = await Client("wrongtype").connect()
    await c.cmd("SET", "lp_wt_key", "value")
    await c.expect(OK)
    await c.cmd("LPUSH", "lp_wt_key", "x")
    await c.expect_error()

    await c.cmd("GET", "lp_wt_key")
    await c.expect(bulk("value"))
    await c.close()


async def test_arity():
    """LPUSH with no elements errors and leaves the connection usable."""
    c = await Client("arity").connect()
    await c.cmd("LPUSH")
    await c.expect_error()
    await c.cmd("LPUSH", "lp_only_key")
    await c.expect_error()

    await c.cmd("LPUSH", "lp_after_key", "ok")
    await c.expect(integer(1))
    await c.close()


async def test_awkward_elements():
    """Elements are opaque bytes, and still reversed correctly."""
    values = ["", '"quoted"', "$5", "*2", "a\r\nb"]
    c = await Client("awkward").connect()
    await c.cmd("LPUSH", "lp_awkward_key", *values)
    await c.expect(integer(len(values)))

    await c.cmd("LRANGE", "lp_awkward_key", "0", "-1")
    await c.expect(resp_array(*reversed(values)))
    await c.close()


async def test_duplicates():
    """Repeated values all count; lists are not sets."""
    c = await Client("dupes").connect()
    await c.cmd("LPUSH", "lp_dupe_key", "same", "same", "same")
    await c.expect(integer(3))
    await c.cmd("LRANGE", "lp_dupe_key", "0", "-1")
    await c.expect(resp_array("same", "same", "same"))
    await c.close()


async def test_expired_key_becomes_fresh_list():
    """A key past its TTL is absent, so LPUSH creates a new list."""
    c = await Client("expired").connect()
    await c.cmd("SET", "lp_ttl_key", "v", "PX", "100")
    await c.expect(OK)
    await asyncio.sleep(0.2)

    await c.cmd("LPUSH", "lp_ttl_key", "fresh")
    await c.expect(integer(1))
    await c.cmd("LRANGE", "lp_ttl_key", "0", "-1")
    await c.expect(resp_array("fresh"))
    await c.close()


async def test_pipelined():
    """Two LPUSHes in one packet keep their order."""
    c = await Client("pipe").connect()
    await c.send(array("LPUSH", "lp_pipe_key", "first")
                 + array("LPUSH", "lp_pipe_key", "second"))
    await c.expect(integer(1))
    await c.expect(integer(2))

    await c.cmd("LRANGE", "lp_pipe_key", "0", "-1")
    await c.expect(resp_array("second", "first"))
    await c.close()


async def test_concurrent():
    """Separate keys under concurrent pushes must not interfere."""

    async def one(n: int):
        c = await Client(f"burst-{n}").connect()
        await c.cmd("LPUSH", f"lp_burst_key_{n}", "x", "y")
        await c.expect(integer(2))
        await c.cmd("LRANGE", f"lp_burst_key_{n}", "0", "-1")
        await c.expect(resp_array("y", "x"))
        await c.close()

    await asyncio.gather(*(one(i) for i in range(1, 5)))


async def test_shared_across_connections():
    """A list built by one connection is visible to another."""
    a = await Client("writer").connect()
    await a.cmd("LPUSH", "lp_shared_key", "a", "b")
    await a.expect(integer(2))
    await a.close()

    b = await Client("reader").connect()
    await b.cmd("LPUSH", "lp_shared_key", "c")
    await b.expect(integer(3))
    await b.cmd("LRANGE", "lp_shared_key", "0", "-1")
    await b.expect(resp_array("c", "b", "a"))
    await b.close()


SCENARIOS = [
    test_create_list,
    test_prepends,
    test_multiple_elements_reverse,
    test_mixed_with_rpush,
    test_negative_range_after_lpush,
    test_wrongtype,
    test_arity,
    test_awkward_elements,
    test_duplicates,
    test_expired_key_becomes_fresh_list,
    test_pipelined,
    test_concurrent,
    test_shared_across_connections,
]

if __name__ == "__main__":
    main(SCENARIOS)