import ipaddress
import json
import logging
import os
import socket
import threading
from dataclasses import dataclass
from multiprocessing.pool import ThreadPool
import time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from taskiq.cli.info_server.models import WorkerState

logger = logging.getLogger("taskiq.worker.info_server")


class ServerState(BaseModel):
    """State of the taskiq server."""

    workers_state: List[WorkerState] = Field(default_factory=list)
    workers_count: int
    active_tasks: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)


class TaskiqInfoServer(threading.Thread):
    def __init__(
        self,
        host: str,
        port: int,
        ready_event: Optional[threading.Event] = None,
        workers_count: int = 0,
    ) -> None:
        super().__init__()
        try:
            addr = ipaddress.ip_address(host)
        except ValueError:
            addr = host
        if isinstance(addr, ipaddress.IPv6Address):
            addr_family = socket.AF_INET6
        else:
            addr_family = socket.AF_INET
        info = socket.getaddrinfo(
            host,
            port,
            family=addr_family,
            type=socket.SOCK_STREAM,
        )
        self.addr_family, self.sock_kind, self.sock_proto, _, self.bind_info = info[0]
        self.stop_event = threading.Event()
        self.state = {}
        self.ready_event = ready_event
        self.state = ServerState(workers_count=workers_count)
        self.methods = {
            "update_state": self.update_state,
        }

    def wait_started(self) -> None:
        if self.ready_event is None:
            return
        while not self.ready_event.is_set():
            time.sleep(0.1)
            if not self.is_alive():
                raise RuntimeError("Failed to start server")

    def wait_workers(self, timeout: Optional[float] = None) -> None:
        start = time.monotonic()
        while True:
            for state in self.state.workers_state:
                if state == WorkerState.READY:
                    break

            if self.state.workers_count == len(self.state.workers_state):
                break

            if timeout is not None and time.monotonic() - start > timeout:
                raise TimeoutError("Failed to start workers")
            print(self.state)
            time.sleep(0.1)

    def kill(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        server = socket.socket(
            self.addr_family,
            self.sock_kind,
            self.sock_proto,
        )
        if os.name != "nt":
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(self.bind_info)
        server.settimeout(1)
        server.listen()
        if self.ready_event is not None:
            self.ready_event.set()
        while True:
            try:
                client, addr = server.accept()
                logger.info(f"Accepted connection from {addr[0]}:{addr[1]}")
                threading.Thread(target=self._handle_client, args=(client,)).start()
            except TimeoutError:
                if self.stop_event.is_set():
                    break
            except Exception as exc:
                logger.warning(
                    "Exception found when processing request %s",
                    exc,
                    exc_info=True,
                )
        server.close()

    def _receive_request(self, client: socket.socket) -> Dict[str, Any]:
        body_len = client.recv(10)
        body_len = int.from_bytes(body_len, "big")
        buffer = b""
        while len(buffer) < body_len:
            buffer += client.recv(1024)
        buffer = buffer[:body_len]
        return json.loads(buffer)

    def _send_response(self, client: socket.socket, data: Dict[str, Any]) -> None:
        encoded = json.dumps(data).encode("utf-8")
        body_len = len(encoded)
        client.sendall(body_len.to_bytes(10, "big") + encoded)

    def _handle_client(self, client: socket.socket) -> None:
        empty_response = {"status": "ok", "data": {}}
        try:
            request = self._receive_request(client)
        except ValueError as exc:
            self._send_response(
                client,
                {"status": "error", "description": str(exc)},
            )
            return
        if "method" not in request and "params" not in request:
            response = {
                "status": "error",
                "description": "Invalid request",
            }
        elif request["method"] not in self.methods:
            response = {
                "status": "error",
                "description": "Unknown method",
            }
        else:
            try:
                response = self.methods[request["method"]](request) or empty_response
            except Exception as exc:
                logger.warning(
                    "Exception found when processing request %s",
                    exc,
                    exc_info=True,
                )
                response = {
                    "status": "error",
                    "description": str(exc),
                }
        self._send_response(client, response)

    def update_state(self, request: Dict[str, Any]) -> None:
        worker_id = request["params"]["worker_id"]
        state = request["params"]["state"]
        self.state.workers_state[worker_id] = WorkerState(state)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ev = threading.Event()
    server = TaskiqInfoServer("127.0.0.1", 2332, ev)
    server.start()
    server.wait_started()
    server.join()
