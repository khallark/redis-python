"""Stages 11-12 - LRANGE (positive indexes, then negative indexes)

Run from the repo root with the server already up:

    python3 stage_testers/11_12.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import OK, Client, array, integer, main, resp_array


async def seed(c, key, *elements):
    await c.cmd("RPUSH", key, *elements)
    await c.expect(integer(len(elements)))


async def test_positive_indexes():
    """Stage 11: LRANGE with positive indexes, stop inclusive."""
    c = await Client("pos").connect()
    await seed(c, "lr_pos_key", "a", "b", "c", "d", "e")

    await c.cmd("LRANGE", "lr_pos_key", "0", "2")
    await c.expect(resp_array("a", "b", "c"))      # inclusive, three not two

    await c.cmd("LRANGE", "lr_pos_key", "1", "3")
    await c.expect(resp_array("b", "c", "d"))

    await c.cmd("LRANGE", "lr_pos_key", "0", "0")
    await c.expect(resp_array("a"))

    await c.cmd("LRANGE", "lr_pos_key", "4", "4")
    await c.expect(resp_array("e"))
    await c.close()


async def test_element_order():
    """RPUSH appends to the right, so insertion order is preserved.

    Nothing before LRANGE could check this: a push that prepended instead
    of appended would have passed every scenario in 8_9_10.py.
    """
    c = await Client("order").connect()
    await c.cmd("RPUSH", "lr_order_key", "first")
    await c.expect(integer(1))
    await c.cmd("RPUSH", "lr_order_key", "second")
    await c.expect(integer(2))
    await c.cmd("RPUSH", "lr_order_key", "third", "fourth")
    await c.expect(integer(4))

    await c.cmd("LRANGE", "lr_order_key", "0", "-1")
    await c.expect(resp_array("first", "second", "third", "fourth"))
    await c.close()


async def test_out_of_range():
    """Indexes past the end clamp; an inverted range is empty."""
    c = await Client("range").connect()
    await seed(c, "lr_range_key", "a", "b", "c")

    await c.cmd("LRANGE", "lr_range_key", "0", "999")
    await c.expect(resp_array("a", "b", "c"))      # stop clamped to n-1

    await c.cmd("LRANGE", "lr_range_key", "1", "999")
    await c.expect(resp_array("b", "c"))

    await c.cmd("LRANGE", "lr_range_key", "10", "20")
    await c.expect(resp_array())                   # start past the end

    await c.cmd("LRANGE", "lr_range_key", "5", "2")
    await c.expect(resp_array())                   # start > stop

    await c.cmd("LRANGE", "lr_range_key", "2", "0")
    await c.expect(resp_array())
    await c.close()


async def test_missing_key():
    """A key that does not exist is an empty array, not null and not an error."""
    c = await Client("missing").connect()
    await c.cmd("LRANGE", "lr_no_such_key", "0", "-1")
    await c.expect(resp_array())
    await c.cmd("LRANGE", "lr_no_such_key", "0", "999")
    await c.expect(resp_array())
    await c.close()


async def test_negative_indexes():
    """Stage 12: -1 is the last element, -2 the one before it."""
    c = await Client("neg").connect()
    await seed(c, "lr_neg_key", "a", "b", "c", "d", "e")

    await c.cmd("LRANGE", "lr_neg_key", "0", "-1")
    await c.expect(resp_array("a", "b", "c", "d", "e"))   # the whole list

    await c.cmd("LRANGE", "lr_neg_key", "-1", "-1")
    await c.expect(resp_array("e"))

    await c.cmd("LRANGE", "lr_neg_key", "-3", "-1")
    await c.expect(resp_array("c", "d", "e"))

    await c.cmd("LRANGE", "lr_neg_key", "-3", "-2")
    await c.expect(resp_array("c", "d"))

    await c.cmd("LRANGE", "lr_neg_key", "0", "-3")
    await c.expect(resp_array("a", "b", "c"))
    await c.close()


async def test_negative_out_of_range():
    """A start below -n clamps to 0; a stop below -n leaves an empty range.

    The second case is the one a lone min(stop, n-1) clamp gets wrong:
    n + stop is still negative, so only the start > stop check catches it.
    """
    c = await Client("negrange").connect()
    await seed(c, "lr_negrange_key", "a", "b", "c")

    await c.cmd("LRANGE", "lr_negrange_key", "-100", "-1")
    await c.expect(resp_array("a", "b", "c"))      # start clamped to 0

    await c.cmd("LRANGE", "lr_negrange_key", "-100", "-50")
    await c.expect(resp_array())                   # stop still negative

    await c.cmd("LRANGE", "lr_negrange_key", "0", "-4")
    await c.expect(resp_array())                   # stop normalises to -1

    await c.cmd("LRANGE", "lr_negrange_key", "-1", "999")
    await c.expect(resp_array("c"))                # mixed signs
    await c.close()


async def test_single_element_list():
    """Boundary arithmetic on a one-element list."""
    c = await Client("single").connect()
    await seed(c, "lr_single_key", "only")

    await c.cmd("LRANGE", "lr_single_key", "0", "-1")
    await c.expect(resp_array("only"))
    await c.cmd("LRANGE", "lr_single_key", "-1", "-1")
    await c.expect(resp_array("only"))
    await c.cmd("LRANGE", "lr_single_key", "1", "5")
    await c.expect(resp_array())
    await c.close()


async def test_awkward_elements_round_trip():
    """Values with RESP metacharacters survive the round trip intact."""
    values = ["", '"quoted"', "$5", "*2", "a\r\nb", "x" * 300]
    c = await Client("awkward").connect()
    await seed(c, "lr_awkward_key", *values)

    await c.cmd("LRANGE", "lr_awkward_key", "0", "-1")
    await c.expect(resp_array(*values))
    await c.close()


async def test_wrongtype():
    """LRANGE against a string key errors and leaves the string intact."""
    c = await Client("wrongtype").connect()
    await c.cmd("SET", "lr_wt_key", "value")
    await c.expect(OK)
    await c.cmd("LRANGE", "lr_wt_key", "0", "-1")
    await c.expect_error()

    await c.cmd("GET", "lr_wt_key")
    from common import bulk
    await c.expect(bulk("value"))
    await c.close()


async def test_bad_arguments():
    """Non-integer indexes and wrong arity error without dropping the socket."""
    c = await Client("bad").connect()
    await seed(c, "lr_bad_key", "a")

    await c.cmd("LRANGE", "lr_bad_key", "a", "b")
    await c.expect_error()
    await c.cmd("LRANGE", "lr_bad_key", "0", "xyz")
    await c.expect_error()
    await c.cmd("LRANGE", "lr_bad_key", "0")
    await c.expect_error()
    await c.cmd("LRANGE")
    await c.expect_error()

    await c.cmd("LRANGE", "lr_bad_key", "0", "-1")
    await c.expect(resp_array("a"))
    await c.close()


async def test_expired_key_is_empty():
    """An expired key reads as absent, so LRANGE is an empty array."""
    c = await Client("expired").connect()
    await c.cmd("SET", "lr_exp_key", "v", "PX", "100")
    await c.expect(OK)
    await asyncio.sleep(0.2)

    await c.cmd("LRANGE", "lr_exp_key", "0", "-1")
    await c.expect(resp_array())
    await c.close()


async def test_pipelined_and_shared():
    """Pipelined reads, and a list visible from another connection."""
    a = await Client("writer").connect()
    await seed(a, "lr_shared_key", "x", "y", "z")
    await a.close()

    b = await Client("reader").connect()
    await b.send(array("LRANGE", "lr_shared_key", "0", "0")
                 + array("LRANGE", "lr_shared_key", "-1", "-1"))
    await b.expect(resp_array("x"))
    await b.expect(resp_array("z"))
    await b.close()


SCENARIOS = [
    test_positive_indexes,
    test_element_order,
    test_out_of_range,
    test_missing_key,
    test_negative_indexes,
    test_negative_out_of_range,
    test_single_element_list,
    test_awkward_elements_round_trip,
    test_wrongtype,
    test_bad_arguments,
    test_expired_key_is_empty,
    test_pipelined_and_shared,
]

if __name__ == "__main__":
    main(SCENARIOS)