from flydelta import Client


def test_server_query(server):
    """Test basic query execution."""
    with Client(server) as client:
        result = client.query("SELECT * FROM users")

        assert result.num_rows == 5
        assert "id" in result.column_names
        assert "name" in result.column_names
        assert "value" in result.column_names


def test_server_query_with_filter(server):
    """Test query with WHERE clause."""
    with Client(server) as client:
        result = client.query("SELECT * FROM users WHERE id > 3")

        assert result.num_rows == 2


def test_server_query_aggregation(server):
    """Test aggregation query."""
    with Client(server) as client:
        result = client.query("SELECT SUM(value) as total FROM users")

        assert result.num_rows == 1
        assert result.column("total")[0].as_py() == 150.0


def test_list_tables(server):
    """Test listing tables."""
    with Client(server) as client:
        tables = client.list_tables()

        assert "users" in tables


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
