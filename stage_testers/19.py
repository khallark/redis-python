"""Stage 19 - TYPE

Run from the repo root with the server already up:

    python3 stage_testers/19.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import OK, PONG, Client, array, bulk, integer, main

STRING = b"+string\r\n"
LIST = b"+list\r\n"
NONE = b"+none\r\n"


async def test_string():
    """Stage 19: a key set with SET is a string."""
    c = await Client("string").connect()
    await c.cmd("SET", "ty_str_key", "value")
    await c.expect(OK)
    await c.cmd("TYPE", "ty_str_key")
    await c.expect(STRING)                  # simple string, not a bulk string
    await c.close()


async def test_list():
    """A key built with RPUSH or LPUSH is a list."""
    c = await Client("list").connect()
    await c.cmd("RPUSH", "ty_rlist_key", "a")
    await c.expect(integer(1))
    await c.cmd("TYPE", "ty_rlist_key")
    await c.expect(LIST)

    await c.cmd("LPUSH", "ty_llist_key", "a")
    await c.expect(integer(1))
    await c.cmd("TYPE", "ty_llist_key")
    await c.expect(LIST)
    await c.close()


async def test_missing_key():
    """A key that never existed is none -- not null and not an error."""
    c = await Client("missing").connect()
    await c.cmd("TYPE", "ty_no_such_key")
    await c.expect(NONE)
    await c.close()


async def test_expired_key():
    """A string past its expiry reports none."""
    c = await Client("expired").connect()
    await c.cmd("SET", "ty_exp_key", "value", "PX", "100")
    await c.expect(OK)
    await c.cmd("TYPE", "ty_exp_key")
    await c.expect(STRING)                  # still alive

    await asyncio.sleep(0.2)
    await c.cmd("TYPE", "ty_exp_key")
    await c.expect(NONE)
    await c.close()


async def test_drained_list():
    """A list emptied by LPOP no longer exists, so it is none."""
    c = await Client("drained").connect()
    await c.cmd("RPUSH", "ty_drain_key", "only")
    await c.expect(integer(1))
    await c.cmd("LPOP", "ty_drain_key")
    await c.expect(bulk("only"))
    await c.cmd("TYPE", "ty_drain_key")
    await c.expect(NONE)
    await c.close()


async def test_type_changes():
    """SET over a list replaces it, and TYPE follows."""
    c = await Client("changes").connect()
    await c.cmd("RPUSH", "ty_swap_key", "a")
    await c.expect(integer(1))
    await c.cmd("TYPE", "ty_swap_key")
    await c.expect(LIST)

    await c.cmd("SET", "ty_swap_key", "now a string")
    await c.expect(OK)
    await c.cmd("TYPE", "ty_swap_key")
    await c.expect(STRING)
    await c.close()


async def test_does_not_modify():
    """TYPE only looks; the value is untouched afterwards."""
    c = await Client("readonly").connect()
    await c.cmd("SET", "ty_ro_key", "value")
    await c.expect(OK)
    await c.cmd("TYPE", "ty_ro_key")
    await c.expect(STRING)
    await c.cmd("GET", "ty_ro_key")
    await c.expect(bulk("value"))
    await c.close()


async def test_arity():
    """TYPE takes exactly one argument; errors keep the connection open."""
    c = await Client("arity").connect()
    await c.cmd("TYPE")
    await c.expect_error()
    await c.cmd("TYPE", "a", "b")
    await c.expect_error()

    await c.cmd("PING")
    await c.expect(PONG)
    await c.close()


async def test_pipelined():
    """Several TYPEs in one packet answer in order."""
    c = await Client("pipe").connect()
    await c.cmd("SET", "ty_pipe_str", "v")
    await c.expect(OK)
    await c.cmd("RPUSH", "ty_pipe_list", "v")
    await c.expect(integer(1))

    await c.send(array("TYPE", "ty_pipe_str")
                 + array("TYPE", "ty_pipe_list")
                 + array("TYPE", "ty_pipe_none"))
    await c.expect(STRING)
    await c.expect(LIST)
    await c.expect(NONE)
    await c.close()


async def test_shared_across_connections():
    """One connection sees the type of a key another created."""
    a = await Client("writer").connect()
    await a.cmd("RPUSH", "ty_shared_key", "v")
    await a.expect(integer(1))
    await a.close()

    b = await Client("reader").connect()
    await b.cmd("TYPE", "ty_shared_key")
    await b.expect(LIST)
    await b.close()


SCENARIOS = [
    test_string,
    test_list,
    test_missing_key,
    test_expired_key,
    test_drained_list,
    test_type_changes,
    test_does_not_modify,
    test_arity,
    test_pipelined,
    test_shared_across_connections,
]

if __name__ == "__main__":
    main(SCENARIOS)
