"""Stages 14-16 - LLEN, LPOP, LPOP with a count

Run from the repo root with the server already up:

    python3 stage_testers/14_15_16.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import NULL, NULL_ARRAY, OK, Client, array, bulk, integer, main, resp_array


async def seed(c, key, *elements):
    await c.cmd("RPUSH", key, *elements)
    await c.expect(integer(len(elements)))


# ----------------------------------------------------------------- LLEN


async def test_llen():
    """Stage 14: LLEN returns the element count."""
    c = await Client("llen").connect()
    await seed(c, "ll_key", "a", "b", "c")
    await c.cmd("LLEN", "ll_key")
    await c.expect(integer(3))

    await c.cmd("RPUSH", "ll_key", "d")
    await c.expect(integer(4))
    await c.cmd("LLEN", "ll_key")
    await c.expect(integer(4))
    await c.close()


async def test_llen_missing_key():
    """LLEN on a key that does not exist is 0, not null and not an error."""
    c = await Client("llen_miss").connect()
    await c.cmd("LLEN", "ll_no_such_key")
    await c.expect(integer(0))
    await c.close()


async def test_llen_wrongtype():
    """LLEN against a string key errors."""
    c = await Client("llen_wt").connect()
    await c.cmd("SET", "ll_wt_key", "value")
    await c.expect(OK)
    await c.cmd("LLEN", "ll_wt_key")
    await c.expect_error()

    await c.cmd("GET", "ll_wt_key")
    await c.expect(bulk("value"))
    await c.close()


async def test_llen_arity():
    """LLEN takes exactly one argument."""
    c = await Client("llen_arity").connect()
    await c.cmd("LLEN")
    await c.expect_error()
    await c.cmd("LLEN", "a", "b")
    await c.expect_error()

    await c.cmd("LLEN", "ll_after_key")
    await c.expect(integer(0))
    await c.close()


# ----------------------------------------------------------------- LPOP


async def test_lpop_single():
    """Stage 15: LPOP removes and returns the head element."""
    c = await Client("lpop").connect()
    await seed(c, "lp_pop_key", "a", "b", "c")

    await c.cmd("LPOP", "lp_pop_key")
    await c.expect(bulk("a"))              # the element, not the new length

    await c.cmd("LRANGE", "lp_pop_key", "0", "-1")
    await c.expect(resp_array("b", "c"))
    await c.cmd("LLEN", "lp_pop_key")
    await c.expect(integer(2))

    await c.cmd("LPOP", "lp_pop_key")
    await c.expect(bulk("b"))
    await c.close()


async def test_lpop_missing_key():
    """LPOP on a missing key is a null bulk string and creates nothing."""
    c = await Client("lpop_miss").connect()
    await c.cmd("LPOP", "lp_ghost_key")
    await c.expect(NULL)

    # the key must not have been brought into existence
    await c.cmd("LLEN", "lp_ghost_key")
    await c.expect(integer(0))
    await c.close()


async def test_lpop_empties_key():
    """Popping the last element deletes the key.

    An empty list does not exist in Redis. If the key survives holding [],
    a later LPUSH would append to it rather than creating a fresh list --
    which happens to look the same here, but LLEN and TYPE would disagree.
    """
    c = await Client("drain").connect()
    await seed(c, "lp_drain_key", "only")

    await c.cmd("LPOP", "lp_drain_key")
    await c.expect(bulk("only"))
    await c.cmd("LLEN", "lp_drain_key")
    await c.expect(integer(0))

    # popping again behaves exactly like a key that never existed
    await c.cmd("LPOP", "lp_drain_key")
    await c.expect(NULL)

    await c.cmd("LPUSH", "lp_drain_key", "new")
    await c.expect(integer(1))
    await c.cmd("LRANGE", "lp_drain_key", "0", "-1")
    await c.expect(resp_array("new"))
    await c.close()


async def test_lpop_wrongtype():
    """LPOP against a string key errors and leaves the string intact."""
    c = await Client("lpop_wt").connect()
    await c.cmd("SET", "lp_wt2_key", "value")
    await c.expect(OK)
    await c.cmd("LPOP", "lp_wt2_key")
    await c.expect_error()
    await c.cmd("GET", "lp_wt2_key")
    await c.expect(bulk("value"))
    await c.close()


# ------------------------------------------------------- LPOP with count


async def test_lpop_count():
    """Stage 16: LPOP key N returns an array of the first N elements."""
    c = await Client("count").connect()
    await seed(c, "lp_count_key", "a", "b", "c", "d", "e")

    await c.cmd("LPOP", "lp_count_key", "2")
    await c.expect(resp_array("a", "b"))       # array, not a bulk string

    await c.cmd("LRANGE", "lp_count_key", "0", "-1")
    await c.expect(resp_array("c", "d", "e"))

    await c.cmd("LPOP", "lp_count_key", "1")
    await c.expect(resp_array("c"))            # still an array, even for one
    await c.close()


async def test_lpop_count_clamps():
    """A count larger than the list returns everything available."""
    c = await Client("clamp").connect()
    await seed(c, "lp_clamp_key", "a", "b")

    await c.cmd("LPOP", "lp_clamp_key", "99")
    await c.expect(resp_array("a", "b"))

    await c.cmd("LLEN", "lp_clamp_key")
    await c.expect(integer(0))                 # drained, so deleted
    await c.close()


async def test_lpop_count_zero():
    """A count of 0 pops nothing and returns the empty array."""
    c = await Client("zero").connect()
    await seed(c, "lp_zero_key", "a", "b")

    await c.cmd("LPOP", "lp_zero_key", "0")
    await c.expect(resp_array())

    await c.cmd("LRANGE", "lp_zero_key", "0", "-1")
    await c.expect(resp_array("a", "b"))       # untouched
    await c.close()


async def test_lpop_count_missing_key():
    """LPOP with a count on a missing key is the null array.

    Redis replies *-1 here rather than the $-1 used by the countless form.
    If the real tester disagrees, a plain empty array is the alternative.
    """
    c = await Client("count_miss").connect()
    await c.cmd("LPOP", "lp_ghost2_key", "3")
    await c.expect(NULL_ARRAY)
    await c.close()


async def test_lpop_bad_count():
    """Negative and non-integer counts error without dropping the socket."""
    c = await Client("badcount").connect()
    await seed(c, "lp_bad_key", "a", "b")

    await c.cmd("LPOP", "lp_bad_key", "-1")
    await c.expect_error()
    await c.cmd("LPOP", "lp_bad_key", "abc")
    await c.expect_error()
    await c.cmd("LPOP", "lp_bad_key", "1", "2")
    await c.expect_error()
    await c.cmd("LPOP")
    await c.expect_error()

    # nothing was popped by any of the failures
    await c.cmd("LRANGE", "lp_bad_key", "0", "-1")
    await c.expect(resp_array("a", "b"))
    await c.close()


# ----------------------------------------------------------------- misc


async def test_interaction_with_push():
    """LPOP and the push commands agree on which end is the head."""
    c = await Client("ends").connect()
    await c.cmd("RPUSH", "lp_ends_key", "middle")
    await c.expect(integer(1))
    await c.cmd("LPUSH", "lp_ends_key", "head")
    await c.expect(integer(2))
    await c.cmd("RPUSH", "lp_ends_key", "tail")
    await c.expect(integer(3))

    await c.cmd("LPOP", "lp_ends_key")
    await c.expect(bulk("head"))
    await c.cmd("LPOP", "lp_ends_key")
    await c.expect(bulk("middle"))
    await c.cmd("LPOP", "lp_ends_key")
    await c.expect(bulk("tail"))
    await c.close()


async def test_awkward_elements():
    """Popped values come back byte-for-byte."""
    values = ["", '"quoted"', "$5", "*2", "a\r\nb"]
    c = await Client("awkward").connect()
    await seed(c, "lp_awk_key", *values)

    await c.cmd("LPOP", "lp_awk_key")
    await c.expect(bulk(""))
    await c.cmd("LPOP", "lp_awk_key", "4")
    await c.expect(resp_array(*values[1:]))
    await c.close()


async def test_pipelined():
    """Pops in one packet return in order."""
    c = await Client("pipe").connect()
    await seed(c, "lp_pipe2_key", "a", "b", "c")

    await c.send(array("LPOP", "lp_pipe2_key")
                 + array("LPOP", "lp_pipe2_key")
                 + array("LLEN", "lp_pipe2_key"))
    await c.expect(bulk("a"))
    await c.expect(bulk("b"))
    await c.expect(integer(1))
    await c.close()


async def test_concurrent():
    """Separate keys under concurrent pops must not interfere."""

    async def one(n: int):
        c = await Client(f"burst-{n}").connect()
        await c.cmd("RPUSH", f"lp_burst2_key_{n}", f"a{n}", f"b{n}")
        await c.expect(integer(2))
        await c.cmd("LPOP", f"lp_burst2_key_{n}")
        await c.expect(bulk(f"a{n}"))
        await c.cmd("LLEN", f"lp_burst2_key_{n}")
        await c.expect(integer(1))
        await c.close()

    await asyncio.gather(*(one(i) for i in range(1, 5)))


async def test_shared_across_connections():
    """One connection pops what another pushed."""
    a = await Client("writer").connect()
    await seed(a, "lp_shared2_key", "x", "y")
    await a.close()

    b = await Client("reader").connect()
    await b.cmd("LPOP", "lp_shared2_key")
    await b.expect(bulk("x"))
    await b.cmd("LLEN", "lp_shared2_key")
    await b.expect(integer(1))
    await b.close()


SCENARIOS = [
    test_llen,
    test_llen_missing_key,
    test_llen_wrongtype,
    test_llen_arity,
    test_lpop_single,
    test_lpop_missing_key,
    test_lpop_empties_key,
    test_lpop_wrongtype,
    test_lpop_count,
    test_lpop_count_clamps,
    test_lpop_count_zero,
    test_lpop_count_missing_key,
    test_lpop_bad_count,
    test_interaction_with_push,
    test_awkward_elements,
    test_pipelined,
    test_concurrent,
    test_shared_across_connections,
]

if __name__ == "__main__":
    main(SCENARIOS)