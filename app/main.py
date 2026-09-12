import asyncio
import time

store: dict[str, object] = {}
expires: dict[str, int] = {}
NULL_BULK = b"$-1\r\n"
NULL_ARRAY = b"*-1\r\n"

def now_ms() -> int:
    return int(time.time() * 1000)

def lookup(key: str):
    """Return the live value for key, deleting it first if it has expired."""
    if key in expires and now_ms() >= expires[key]:
        store.pop(key, None)
        expires.pop(key, None)
    return store.get(key)

def RESP_bulk_string(string: str) -> bytes:
    return f"${len(string)}\r\n{string}\r\n".encode('utf-8')

def RESP_integer(n: int) -> bytes:
    return f":{n}\r\n".encode('utf-8')

def RESP_array(items: list[str]) -> bytes:
    out = [f"*{len(items)}\r\n".encode('utf-8')]
    for item in items:
        out.append(RESP_bulk_string(item))
    return b"".join(out)

def RESP_error(message: str) -> bytes:
    return f"-ERR {message}\r\n".encode('utf-8')

def RESP_list_error(message: str) -> bytes:
    return f"-WRONGTYPE {message}\r\n".encode('utf-8')

def RESP_parse_one(buf: bytes) -> tuple[list[str] | None, int]:
    """Extract one complete command from buf.

    Returns (tokens, bytes_consumed), or (None, 0) if buf does not yet hold a
    complete command.
    """
    if not buf:
        return None, 0
    if not buf.startswith(b'*'):
        raise ValueError(f"expected an array, got {buf[:1]!r}")

    stop = buf.find(b'\r\n')
    if stop == -1:
        return None, 0
    count = int(buf[1:stop])

    pos = stop + 2
    tokens = []
    for _ in range(count):
        if buf[pos:pos + 1] != b'$':
            return None, 0
        stop = buf.find(b'\r\n', pos)
        if stop == -1:
            return None, 0
        length = int(buf[pos + 1:stop])
        start = stop + 2
        if len(buf) < start + length + 2:
            return None, 0
        tokens.append(buf[start:start + length].decode('utf-8'))
        pos = start + length + 2

    return tokens, pos
    
async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    # Get the unique port of the connecting client
    client_port = writer.get_extra_info('peername')[1]
    print(f"📥 Connected: Client {client_port}")

    buffer = b""
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            buffer += data

            while True:
                tokens, consumed = RESP_parse_one(buffer)
                if tokens is None:
                    break
                buffer = buffer[consumed:]

                if not tokens:
                    continue

                command = tokens[0].upper()
                args = tokens[1:]

                match command:
                    case 'PING':
                        writer.write(b"+PONG\r\n")
                    case 'ECHO':
                        if len(args) != 1:
                            writer.write(RESP_error("wrong number of arguments for 'echo' command"))
                        else:
                            writer.write(RESP_bulk_string(args[0]))
                    case 'SET':
                        if len(args) < 2:
                            writer.write(RESP_error("wrong number of arguments for 'set' command"))
                        else:
                            key, value = args[0], args[1]
                            deadline = None
                            bad = None
                            i = 2
                            while i < len(args):
                                opt = args[i].upper()
                                if opt in ['EX', 'PX', 'EXAT', 'PXAT']:
                                    if i + 1 >= len(args):
                                        bad = "syntax error"
                                        break
                                    try:
                                        n = int(args[i + 1])
                                    except ValueError:
                                        bad = "value is not an integer or out of range"
                                        break
                                    deadline = {
                                        'EX':   now_ms() + n * 1000,
                                        'PX':   now_ms() + n,
                                        'EXAT': n * 1000,
                                        'PXAT': n,
                                    }[opt]
                                    i += 2
                                else:
                                    bad = "syntax error"
                                    break
                            
                            if bad:
                                writer.write(RESP_error(bad))
                            else:
                                store[key] = value
                                expires.pop(key, None)
                                if deadline is not None:
                                    expires[key] = deadline
                                writer.write(b"+OK\r\n")
                    case 'GET':
                        if len(args) != 1:
                            writer.write(RESP_error("wrong number of arguments for 'get' command"))
                        else:
                            value = lookup(args[0])
                            if value is not None:
                                writer.write(RESP_bulk_string(value))
                            else:
                                writer.write(NULL_BULK)
                    case 'RPUSH':
                        if len(args) < 2:
                            writer.write(RESP_error("wrong number of arguments for 'rpush' command"))
                        else:
                            key, values = args[0], args[1:]
                            current = lookup(key)
                            if current is not None and not isinstance(current, list):
                                writer.write(RESP_list_error("Operation against a key holding the wrong kind of value"))
                            else:
                                if current is None:
                                    current = []
                                    store[key] = current
                                current.extend(values)
                                writer.write(RESP_integer(len(current)))
                    case 'LPUSH':
                        if len(args) < 2:
                            writer.write(RESP_error("wrong number of arguments for 'lpush' command"))
                        else:
                            key, values = args[0], args[1:]
                            current = lookup(key)
                            if current is not None and not isinstance(current, list):
                                writer.write(RESP_list_error("Operation against a key holding the wrong kind of value"))
                            else:
                                if current is None:
                                    current = []
                                    store[key] = current
                                values.reverse()
                                current[0:0] = values
                                writer.write(RESP_integer(len(current)))
                    case 'LLEN':
                        if len(args) != 1:
                            writer.write(RESP_error("wrong number of arguments for 'llen' command"))
                        else:
                            current = lookup(args[0])
                            if current is None:
                                writer.write(RESP_integer(0))
                            elif not isinstance(current, list):
                                writer.write(RESP_list_error("Operation against a key holding the wrong kind of value"))
                            else:
                                writer.write(RESP_integer(len(current)))
                    case 'LPOP':
                        if len(args) < 1 or len(args) > 2:
                            writer.write(RESP_error("wrong number of arguments for 'lpop' command"))
                        else:
                            key = args[0]
                            count = None
                            bad = None
                            
                            if len(args) == 2:
                                try:
                                    count = int(args[1])
                                except ValueError:
                                    bad = "value is not an integer or out of range"
                                else:
                                    if count < 0:
                                        bad = "value is out of range, must be positive"
                            if bad:
                                writer.write(RESP_error(bad))
                            else:
                                current = lookup(key)
                                if current is not None and not isinstance(current, list):
                                    writer.write(RESP_list_error("Operation against a key holding the wrong kind of value"))
                                elif current is None:
                                    writer.write(NULL_ARRAY if count is not None else NULL_BULK)
                                elif count is None:
                                    popped = current.pop(0)
                                    if not current:
                                        store.pop(key, None)
                                        expires.pop(key, None)
                                    writer.write(RESP_bulk_string(popped))
                                else:
                                    popped = current[:count]
                                    del current[:count]
                                    if not current:
                                        store.pop(key, None)
                                        expires.pop(key, None)
                                    writer.write(RESP_array(popped))
                    case 'LRANGE':
                        if len(args) != 3:
                            writer.write(RESP_error("wrong number of arguments for 'lrange' command"))
                        else:
                            key = args[0]
                            try:
                                start = int(args[1])
                                stop = int(args[2])
                            except ValueError:
                                writer.write(RESP_error("value is not an integer or out of range"))
                            else:
                                current = lookup(key)
                                if current is not None and not isinstance(current, list):
                                    writer.write(RESP_list_error("Operation against a key holding the wrong kind of value"))
                                else:
                                    if current is None:
                                        current = []
                                    n = len(current)
                                    if start < 0:
                                        start = max(n + start, 0)
                                    if stop < 0:
                                        stop = n + stop
                                    stop = min(stop, n - 1)
                                    if start > stop:
                                        writer.write(RESP_array([]))
                                    else:
                                        writer.write(RESP_array(current[ start : stop + 1 ]))
                    case _:
                        writer.write(RESP_error(f"unknown command '{tokens[0]}'"))

                await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    print("🚀 Custom Redis running on port 6379...")
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Server stopped.")
