"""Stages 17-18 - BLPOP, BLPOP with a timeout

Run from the repo root with the server already up:

    python3 stage_testers/17_18.py
"""

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import NULL_ARRAY, OK, PONG, Client, array, bulk, fail, integer, main, resp_array

SETTLE = 0.1        # time for a sent BLPOP to reach the server and block


async def seed(c, key, *elements):
    await c.cmd("RPUSH", key, *elements)
    await c.expect(integer(len(elements)))


# ------------------------------------------------------------ immediate


async def test_immediate():
    """BLPOP on a non-empty list pops at once, like LPOP, but replies [key, element]."""
    c = await Client("immediate").connect()
    await seed(c, "bl_now_key", "a", "b")

    await c.cmd("BLPOP", "bl_now_key", "0")
    await c.expect(resp_array("bl_now_key", "a"))

    await c.cmd("LRANGE", "bl_now_key", "0", "-1")
    await c.expect(resp_array("b"))
    await c.close()


async def test_first_non_empty_key():
    """With several keys, the first one holding elements is served."""
    c = await Client("multikey").connect()
    await seed(c, "bl_mk_second", "x")

    await c.cmd("BLPOP", "bl_mk_first", "bl_mk_second", "0")
    await c.expect(resp_array("bl_mk_second", "x"))     # reply names the key
    await c.close()


# ------------------------------------------------------------- blocking


async def test_blocks_until_push():
    """Stage 17: BLPOP on an empty key waits for another client's push."""
    waiter = await Client("waiter").connect()
    pusher = await Client("pusher").connect()

    await waiter.cmd("BLPOP", "bl_block_key", "0")
    await asyncio.sleep(SETTLE)

    await pusher.cmd("RPUSH", "bl_block_key", "v")
    await pusher.expect(integer(1))                     # length counts the served element
    await waiter.expect(resp_array("bl_block_key", "v"))

    # the element went to the waiter, so the key is gone
    await pusher.cmd("LLEN", "bl_block_key")
    await pusher.expect(integer(0))
    await waiter.close()
    await pusher.close()


async def test_woken_by_lpush():
    """LPUSH wakes a blocked client too."""
    waiter = await Client("waiter").connect()
    pusher = await Client("pusher").connect()

    await waiter.cmd("BLPOP", "bl_lpush_key", "0")
    await asyncio.sleep(SETTLE)

    await pusher.cmd("LPUSH", "bl_lpush_key", "v")
    await pusher.expect(integer(1))
    await waiter.expect(resp_array("bl_lpush_key", "v"))
    await waiter.close()
    await pusher.close()


async def test_fifo_waiters():
    """The client that has blocked longest is served first."""
    first = await Client("first").connect()
    second = await Client("second").connect()
    pusher = await Client("pusher").connect()

    await first.cmd("BLPOP", "bl_fifo_key", "0")
    await asyncio.sleep(SETTLE)
    await second.cmd("BLPOP", "bl_fifo_key", "0")
    await asyncio.sleep(SETTLE)

    await pusher.cmd("RPUSH", "bl_fifo_key", "one", "two")
    await pusher.expect(integer(2))
    await first.expect(resp_array("bl_fifo_key", "one"))
    await second.expect(resp_array("bl_fifo_key", "two"))
    await first.close()
    await second.close()
    await pusher.close()


async def test_no_stale_waiter():
    """A client blocked on two keys and served by one stops waiting on the other.

    If its registration on the other key lingered, a later push there would
    be handed to a client that is no longer listening, and vanish.
    """
    waiter = await Client("waiter").connect()
    pusher = await Client("pusher").connect()

    await waiter.cmd("BLPOP", "bl_stale_a", "bl_stale_b", "0")
    await asyncio.sleep(SETTLE)

    await pusher.cmd("RPUSH", "bl_stale_a", "x")
    await pusher.expect(integer(1))
    await waiter.expect(resp_array("bl_stale_a", "x"))

    await pusher.cmd("RPUSH", "bl_stale_b", "y")
    await pusher.expect(integer(1))
    await pusher.cmd("LRANGE", "bl_stale_b", "0", "-1")
    await pusher.expect(resp_array("y"))                # still in the list
    await waiter.close()
    await pusher.close()


# -------------------------------------------------------------- timeout


async def test_timeout():
    """Stage 18: with nothing pushed, BLPOP replies the null array after the timeout."""
    c = await Client("timeout").connect()

    start = time.monotonic()
    await c.cmd("BLPOP", "bl_to_key", "0.3")
    await c.expect(NULL_ARRAY)
    elapsed = time.monotonic() - start
    if elapsed < 0.25:
        fail("timeout", f"replied after {elapsed:.2f}s, before the 0.3s timeout")

    # the connection is still usable afterwards
    await c.cmd("PING")
    await c.expect(PONG)
    await c.close()


async def test_push_before_timeout():
    """A push that lands within the timeout is delivered, not the null array."""
    waiter = await Client("waiter").connect()
    pusher = await Client("pusher").connect()

    await waiter.cmd("BLPOP", "bl_in_time_key", "1")
    await asyncio.sleep(SETTLE)

    await pusher.cmd("RPUSH", "bl_in_time_key", "v")
    await pusher.expect(integer(1))
    await waiter.expect(resp_array("bl_in_time_key", "v"))
    await waiter.close()
    await pusher.close()


async def test_push_after_timeout():
    """Once a BLPOP has timed out, a later push stays in the list."""
    c = await Client("late").connect()
    await c.cmd("BLPOP", "bl_late_key", "0.1")
    await c.expect(NULL_ARRAY)

    await c.cmd("RPUSH", "bl_late_key", "v")
    await c.expect(integer(1))
    await c.cmd("LLEN", "bl_late_key")
    await c.expect(integer(1))
    await c.close()


# ----------------------------------------------------------- disconnect


async def test_disconnect_while_blocked():
    """A client that hangs up while blocked does not swallow the next push."""
    gone = await Client("gone").connect()
    await gone.cmd("BLPOP", "bl_dc_key", "0")
    await asyncio.sleep(SETTLE)
    await gone.close()
    await asyncio.sleep(SETTLE)

    c = await Client("after").connect()
    await c.cmd("RPUSH", "bl_dc_key", "keep")
    await c.expect(integer(1))
    await c.cmd("LRANGE", "bl_dc_key", "0", "-1")
    await c.expect(resp_array("keep"))
    await c.close()


async def test_live_waiter_behind_dead_one():
    """A disconnected waiter ahead in the queue is skipped."""
    gone = await Client("gone").connect()
    live = await Client("live").connect()

    await gone.cmd("BLPOP", "bl_dc2_key", "0")
    await asyncio.sleep(SETTLE)
    await live.cmd("BLPOP", "bl_dc2_key", "0")
    await asyncio.sleep(SETTLE)
    await gone.close()
    await asyncio.sleep(SETTLE)

    pusher = await Client("pusher").connect()
    await pusher.cmd("RPUSH", "bl_dc2_key", "v")
    await pusher.expect(integer(1))
    await live.expect(resp_array("bl_dc2_key", "v"))
    await live.close()
    await pusher.close()


# ---------------------------------------------------------------- misc


async def test_pipelined_behind_block():
    """A command sent in the same packet as a blocking BLPOP runs after it."""
    c = await Client("pipe").connect()
    pusher = await Client("pusher").connect()

    await c.send(array("BLPOP", "bl_pipe_key", "0") + array("ECHO", "after"))
    await asyncio.sleep(SETTLE)

    await pusher.cmd("RPUSH", "bl_pipe_key", "v")
    await pusher.expect(integer(1))
    await c.expect(resp_array("bl_pipe_key", "v"))
    await c.expect(bulk("after"))
    await c.close()
    await pusher.close()


async def test_errors():
    """Bad arguments error without blocking or dropping the socket."""
    c = await Client("errors").connect()
    await c.cmd("SET", "bl_str_key", "value")
    await c.expect(OK)

    await c.cmd("BLPOP", "bl_str_key", "0")         # wrong type
    await c.expect_error()
    await c.cmd("BLPOP", "bl_err_key", "abc")       # timeout not a number
    await c.expect_error()
    await c.cmd("BLPOP", "bl_err_key", "-1")        # negative timeout
    await c.expect_error()
    await c.cmd("BLPOP", "bl_err_key")              # no timeout
    await c.expect_error()

    await c.cmd("PING")
    await c.expect(PONG)
    await c.close()


SCENARIOS = [
    test_immediate,
    test_first_non_empty_key,
    test_blocks_until_push,
    test_woken_by_lpush,
    test_fifo_waiters,
    test_no_stale_waiter,
    test_timeout,
    test_push_before_timeout,
    test_push_after_timeout,
    test_disconnect_while_blocked,
    test_live_waiter_behind_dead_one,
    test_pipelined_behind_block,
    test_errors,
]

if __name__ == "__main__":
    main(SCENARIOS)
