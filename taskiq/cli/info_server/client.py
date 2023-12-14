import asyncio
import ipaddress
import json
import socket
from typing import Any, Dict
import contextlib


def sync_send_request(host: str, port: int, data: Dict[str, Any]) -> str:
    with contextlib.suppress(ValueError):
        host = ipaddress.ip_address(host)
    if isinstance(host, ipaddress.IPv6Address):
        addr_family = socket.AF_INET6
    else:
        addr_family = socket.AF_INET
    info = socket.getaddrinfo(
        host,
        port,
        family=addr_family,
        type=socket.SOCK_STREAM,
    )
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    encoded = json.dumps(data).encode("utf-8")
    sock.sendall(len(encoded).to_bytes(10, "big") + encoded)


class TaskiqServerClient:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    async def read_response(self, reader: asyncio.StreamReader) -> Dict[str, Any]:
        response = await reader.read(10)
        if response == b"":
            raise ConnectionError("Connection closed")
        body_len = int.from_bytes(response, "big")
        buffer = b""
        while len(buffer) < body_len:
            buffer += await reader.read(1024)
        return json.loads(buffer[:body_len])

    async def send_request(self, data: Dict[str, Any]) -> str:
        reader, writer = await asyncio.open_connection(self.host, self.port)
        body = json.dumps(data)
        body_len = len(body)
        writer.write(body_len.to_bytes(10, "big") + body.encode("utf-8"))
        await writer.drain()
        return await self.read_response(reader)
