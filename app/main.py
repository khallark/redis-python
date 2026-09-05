import asyncio

def RESP_parse_one(buf: bytes) -> tuple[list[str] | None, int]:
    """Extract one complete command from buf.

    Returns (tokens, bytes_consumed), or (None, 0) if buf does not yet hold a
    complete command.
    """
    if not buf:
        return None, 0
    if not buf.startswith(b'*'):
        raise ValueError(f"expected an array, got {buf[:1]!r}")

    end = buf.find(b'\r\n')
    if end == -1:
        return None, 0
    count = int(buf[1:end])

    pos = end + 2
    tokens = []
    for _ in range(count):
        if buf[pos:pos + 1] != b'$':
            return None, 0
        end = buf.find(b'\r\n', pos)
        if end == -1:
            return None, 0
        length = int(buf[pos + 1:end])
        start = end + 2
        if len(buf) < start + length + 2:
            return None, 0
        tokens.append(buf[start:start + length].decode('utf-8'))
        pos = start + length + 2

    return tokens, pos

def RESP_bulk_string(string: str) -> bytes:
    return f"${len(string)}\r\n{string}\r\n".encode('utf-8')

def RESP_error(message: str) -> bytes:
    return f"-ERR {message}\r\n".encode('utf-8')
    
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
