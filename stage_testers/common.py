"""Shared machinery for the per-stage testers.

Each stage_testers/N.py imports from here and only defines its scenarios.
"""

import asyncio
import sys
import time

HOST = "localhost"
PORT = 6379
TIMEOUT = 2.0

START = time.monotonic()
FAILURES: list[str] = []


# ---------------------------------------------------------------- logging

def log(who: str, arrow: str, msg: str) -> None:
    elapsed = time.monotonic() - START
    print(f"{elapsed:7.3f}s  {who:<9} {arrow} {msg}")


def fail(who: str, msg: str) -> None:
    FAILURES.append(f"{who}: {msg}")
    log(who, "!!", msg)


# ---------------------------------------------------------------- encoding

def array(*parts: str) -> bytes:
    """Encode parts as a RESP array of bulk strings."""
    out = [f"*{len(parts)}\r\n".encode()]
    for p in parts:
        out.append(b"$%d\r\n%s\r\n" % (len(p), p.encode()))
    return b"".join(out)


def bulk(s: str) -> bytes:
    return f"${len(s)}\r\n{s}\r\n".encode()


def resp_array(*items: str) -> bytes:
    """Encode an array reply of bulk strings. No args -> the empty array."""
    out = [f"*{len(items)}\r\n".encode()]
    for item in items:
        out.append(bulk(item))
    return b"".join(out)


def integer(n: int) -> bytes:
    return f":{n}\r\n".encode()


OK = b"+OK\r\n"
PONG = b"+PONG\r\n"
NULL = b"$-1\r\n"


# ---------------------------------------------------------------- decoding

SHOW_RAW = False        # set True to append the raw bytes to every log line


def quote(s: str) -> str:
    """redis-cli style quoting: control characters stay visible."""
    body = (s.replace("\\", "\\\\")
             .replace('"', '\\"')
             .replace("\r", "\\r")
             .replace("\n", "\\n")
             .replace("\t", "\\t"))
    return f'"{body}"'


def _decode_one(buf: bytes) -> tuple[str | None, int]:
    """Render the first RESP value in buf. Returns (text, consumed)."""
    if not buf:
        return None, 0

    kind = buf[:1]
    end = buf.find(b"\r\n")
    if end == -1:
        return None, 0
    head = buf[1:end].decode("utf-8", "replace")

    if kind == b"+":
        return head, end + 2
    if kind == b"-":
        return f"(error) {head}", end + 2
    if kind == b":":
        return f"(integer) {head}", end + 2

    if kind == b"$":
        n = int(head)
        if n == -1:
            return "(nil)", end + 2
        start = end + 2
        if len(buf) < start + n + 2:
            return None, 0
        return quote(buf[start:start + n].decode("utf-8", "replace")), start + n + 2

    if kind == b"*":
        n = int(head)
        if n == -1:
            return "(nil array)", end + 2
        pos = end + 2
        items = []
        for _ in range(n):
            text, used = _decode_one(buf[pos:])
            if text is None:
                return None, 0
            items.append(text)
            pos += used
        return items, pos

    return None, 0


def render_lines(payload: bytes, command: bool = False) -> list[str]:
    """Turn RESP bytes into one readable line per value.

    A payload holding several values (a pipelined write) produces several
    lines, each tagged so it is clear they shared one packet. Anything left
    over is an incomplete fragment and is shown raw.
    """
    parts, pos = [], 0
    while pos < len(payload):
        value, used = _decode_one(payload[pos:])
        if value is None:
            break
        pos += used
        if isinstance(value, list):
            if command and value:
                parts.append(" ".join([value[0].strip('"')] + value[1:]))
            else:
                parts.append("[" + ", ".join(str(v) for v in value) + "]")
        else:
            parts.append(str(value))

    if pos < len(payload):
        leftover = payload[pos:]
        parts.append(f"<partial {leftover.decode('utf-8', 'replace')!r}>")

    if not parts:
        parts = [repr(payload.decode("utf-8", "replace"))]

    if len(parts) > 1:
        parts = [f"{p}   [same packet]" for p in parts]
    if SHOW_RAW:
        raw = repr(payload.decode("utf-8", "replace"))
        parts = [f"{p}   {raw}" for p in parts]
    return parts


def render(payload: bytes, command: bool = False) -> str:
    """Single-line form, for failure messages."""
    return " ".join(render_lines(payload, command))


# ---------------------------------------------------------------- client

class Client:
    def __init__(self, name: str):
        self.name = name
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def connect(self) -> "Client":
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(HOST, PORT), TIMEOUT
        )
        log(self.name, "--", f"connected to {HOST}:{PORT}")
        return self

    async def send(self, payload: bytes) -> None:
        self.writer.write(payload)
        await self.writer.drain()
        for line in render_lines(payload, command=True):
            log(self.name, ">>", line)

    async def cmd(self, *parts: str) -> None:
        await self.send(array(*parts))

    async def expect(self, want: bytes) -> None:
        """Read exactly len(want) bytes and compare."""
        try:
            got = await asyncio.wait_for(self.reader.readexactly(len(want)), TIMEOUT)
        except asyncio.TimeoutError:
            fail(self.name, f"timed out waiting for {render(want)}")
            return
        except asyncio.IncompleteReadError as e:
            fail(self.name, f"server closed early, got {render(e.partial)}")
            return

        if got == want:
            for line in render_lines(got):
                log(self.name, "<<", line)
        else:
            fail(self.name, f"wanted {render(want)}, got {render(got)}")

    async def expect_error(self) -> None:
        """Read one line and check it is a RESP error. Text is not compared."""
        try:
            got = await asyncio.wait_for(self.reader.readuntil(b"\r\n"), TIMEOUT)
        except asyncio.TimeoutError:
            fail(self.name, "timed out waiting for an error reply")
            return
        except asyncio.IncompleteReadError as e:
            fail(self.name, f"server closed early, got {render(e.partial)}")
            return

        if got.startswith(b"-"):
            for line in render_lines(got):
                log(self.name, "<<", line)
        else:
            fail(self.name, f"wanted an error reply, got {render(got)}")

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()
        log(self.name, "--", "disconnected")


# ---------------------------------------------------------------- runner

async def run(scenarios) -> int:
    print(f"--- target {HOST}:{PORT} ---")
    for scenario in scenarios:
        print(f"\n--- {scenario.__name__.removeprefix('test_')} ---")
        if scenario.__doc__:
            print(f"    {scenario.__doc__.strip().splitlines()[0]}")
        try:
            await scenario()
        except (ConnectionError, OSError, asyncio.TimeoutError) as e:
            fail(scenario.__name__, f"{type(e).__name__}: {e}")

    print()
    if FAILURES:
        print(f"--- {len(FAILURES)} failure(s) ---")
        for f in FAILURES:
            print(f"  {f}")
        return 1
    print("--- all checks passed ---")
    return 0


def main(scenarios) -> None:
    sys.exit(asyncio.run(run(scenarios)))