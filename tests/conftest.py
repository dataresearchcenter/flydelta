import tempfile
import threading
import time

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from flydelta import Client, Server


@pytest.fixture
def delta_table_path():
    """Create a temporary Delta Lake table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        table = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["alice", "bob", "charlie", "david", "eve"],
                "value": [10.0, 20.0, 30.0, 40.0, 50.0],
                "active": [True, False, True, False, True],
            }
        )
        write_deltalake(tmpdir, table)
        yield tmpdir


@pytest.fixture
def large_delta_table_path():
    """Create a larger Delta Lake table for streaming tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        table = pa.table(
            {
                "id": list(range(10000)),
                "value": [float(i) for i in range(10000)],
            }
        )
        write_deltalake(tmpdir, table)
        yield tmpdir


@pytest.fixture
def server(delta_table_path):
    """Start a server with a test table named 'users'."""
    location = "grpc://127.0.0.1:18815"
    server = Server(
        location=location,
        tables={"users": delta_table_path},
    )

    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.5)

    yield location

    server.shutdown()


@pytest.fixture
def server_with_large_table(large_delta_table_path):
    """Start a server with a larger table for streaming tests."""
    location = "grpc://127.0.0.1:18816"
    server = Server(
        location=location,
        tables={"large_table": large_delta_table_path},
        batch_size=1000,
    )

    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.5)

    yield location

    server.shutdown()


@pytest.fixture
def client(server):
    """Create a client connected to the test server."""
    with Client(server) as client:
        yield client
