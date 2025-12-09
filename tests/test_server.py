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
        table = pa.table({
            "id": [1, 2, 3, 4, 5],
            "name": ["alice", "bob", "charlie", "david", "eve"],
            "value": [10.0, 20.0, 30.0, 40.0, 50.0],
        })
        write_deltalake(tmpdir, table)
        yield tmpdir


@pytest.fixture
def large_delta_table_path():
    """Create a larger Delta Lake table for streaming tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        table = pa.table({
            "id": list(range(10000)),
            "value": [float(i) for i in range(10000)],
        })
        write_deltalake(tmpdir, table)
        yield tmpdir


@pytest.fixture
def server_with_table(delta_table_path):
    """Start a server with a test table."""
    location = "grpc://127.0.0.1:18815"
    server = Server(
        location=location,
        tables={"test_table": delta_table_path},
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


def test_server_query(server_with_table):
    """Test basic query execution."""
    with Client(server_with_table) as client:
        result = client.query("SELECT * FROM test_table")

        assert result.num_rows == 5
        assert "id" in result.column_names
        assert "name" in result.column_names
        assert "value" in result.column_names


def test_server_query_with_filter(server_with_table):
    """Test query with WHERE clause."""
    with Client(server_with_table) as client:
        result = client.query("SELECT * FROM test_table WHERE id > 3")

        assert result.num_rows == 2


def test_server_query_aggregation(server_with_table):
    """Test aggregation query."""
    with Client(server_with_table) as client:
        result = client.query("SELECT SUM(value) as total FROM test_table")

        assert result.num_rows == 1
        assert result.column("total")[0].as_py() == 150.0


def test_list_tables(server_with_table):
    """Test listing tables."""
    with Client(server_with_table) as client:
        tables = client.list_tables()

        assert "test_table" in tables


def test_stream_query(server_with_large_table):
    """Test streaming query returns batches."""
    with Client(server_with_large_table) as client:
        batches = list(client.stream_query("SELECT * FROM large_table"))

        assert len(batches) > 1  # Should be multiple batches
        total_rows = sum(batch.num_rows for batch in batches)
        assert total_rows == 10000


def test_stream_query_with_filter(server_with_large_table):
    """Test streaming query with filter."""
    with Client(server_with_large_table) as client:
        total_rows = 0
        for batch in client.stream_query("SELECT * FROM large_table WHERE id < 500"):
            total_rows += batch.num_rows

        assert total_rows == 500


def test_stream_query_processes_incrementally(server_with_large_table):
    """Test that streaming processes data incrementally."""
    with Client(server_with_large_table) as client:
        batch_count = 0
        for batch in client.stream_query("SELECT * FROM large_table"):
            batch_count += 1
            # Each batch should be <= batch_size (1000)
            assert batch.num_rows <= 1000

        assert batch_count >= 10  # 10000 rows / 1000 batch_size
