import pyarrow as pa

from flydelta import Client


def test_client_query_returns_arrow_table(client):
    """Test that query returns a PyArrow Table."""
    result = client.query("SELECT * FROM users")

    assert isinstance(result, pa.Table)
    assert result.num_rows == 5


def test_client_query_to_pandas(client):
    """Test converting Arrow table to pandas DataFrame."""
    result = client.query("SELECT * FROM users")
    df = result.to_pandas()

    assert df.__class__.__name__ == "DataFrame"
    assert df.__class__.__module__ == "pandas.core.frame"
    assert len(df) == 5
    assert list(df.columns) == ["id", "name", "value", "active"]


def test_client_query_with_filter(client):
    """Test query with WHERE clause."""
    result = client.query("SELECT * FROM users WHERE active = true")
    df = result.to_pandas()

    assert len(df) == 3
    assert all(df["active"])


def test_client_list_tables(client):
    """Test listing available tables."""
    tables = client.list_tables()

    assert isinstance(tables, list)
    assert "users" in tables


def test_client_stream_query_yields_batches(client):
    """Test that stream_query yields RecordBatches."""
    batches = list(client.stream_query("SELECT * FROM users"))

    assert len(batches) >= 1
    assert all(isinstance(b, pa.RecordBatch) for b in batches)
    total_rows = sum(b.num_rows for b in batches)
    assert total_rows == 5


def test_client_context_manager(server):
    """Test client works as context manager."""
    with Client(server) as client:
        result = client.query("SELECT COUNT(*) as cnt FROM users")
        assert result.column("cnt")[0].as_py() == 5


def test_client_empty_result(client):
    """Test query returning empty result."""
    result = client.query("SELECT * FROM users WHERE id > 100")

    assert result.num_rows == 0


def test_client_column_selection(client):
    """Test selecting specific columns."""
    result = client.query("SELECT id, name FROM users")

    assert result.num_columns == 2
    assert result.column_names == ["id", "name"]


def test_client_ordering(client):
    """Test ORDER BY clause."""
    result = client.query("SELECT * FROM users ORDER BY value DESC")
    df = result.to_pandas()

    assert df.iloc[0]["value"] == 50.0
    assert df.iloc[-1]["value"] == 10.0
