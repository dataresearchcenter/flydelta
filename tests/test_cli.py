from typer.testing import CliRunner

from flydelta import __version__
from flydelta.cli import cli

runner = CliRunner()


def test_cli_version():
    """Test --version flag shows version."""
    result = runner.invoke(cli, ["--version"])

    assert result.exit_code == 0
    assert __version__ in result.output


def test_cli_help():
    """Test --help shows usage information."""
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "serve" in result.output
    assert "query" in result.output
    assert "tables" in result.output


def test_cli_serve_help():
    """Test serve --help shows options."""
    result = runner.invoke(cli, ["serve", "--help"])

    assert result.exit_code == 0
    assert "--host" in result.output
    assert "--port" in result.output
    assert "--table" in result.output
    assert "--pool-size" in result.output
    assert "--batch-size" in result.output


def test_cli_serve_invalid_table_format():
    """Test serve with invalid table format."""
    result = runner.invoke(cli, ["serve", "-t", "invalid_format"])

    assert result.exit_code == 1
    assert "Invalid table format" in result.output


def test_cli_query_help():
    """Test query --help shows options."""
    result = runner.invoke(cli, ["query", "--help"])

    assert result.exit_code == 0
    assert "--host" in result.output
    assert "--port" in result.output
    assert "--output" in result.output


def test_cli_tables_help():
    """Test tables --help shows options."""
    result = runner.invoke(cli, ["tables", "--help"])

    assert result.exit_code == 0
    assert "--host" in result.output
    assert "--port" in result.output


def test_cli_query_with_server(server):
    """Test query command against running server."""
    result = runner.invoke(
        cli,
        [
            "query",
            "SELECT COUNT(*) as cnt FROM users",
            "-h",
            "127.0.0.1",
            "-p",
            "18815",
        ],
    )

    assert result.exit_code == 0
    assert "cnt" in result.output
    assert "5" in result.output


def test_cli_query_json_output(server):
    """Test query command with JSON output."""
    result = runner.invoke(
        cli,
        [
            "query",
            "SELECT id, name FROM users ORDER BY id LIMIT 2",
            "-h",
            "127.0.0.1",
            "-p",
            "18815",
            "-o",
            "json",
        ],
    )

    assert result.exit_code == 0
    assert "alice" in result.output
    assert "bob" in result.output


def test_cli_query_csv_output(server):
    """Test query command with CSV output."""
    result = runner.invoke(
        cli,
        [
            "query",
            "SELECT id, name FROM users ORDER BY id LIMIT 2",
            "-h",
            "127.0.0.1",
            "-p",
            "18815",
            "-o",
            "csv",
        ],
    )

    assert result.exit_code == 0
    assert "id,name" in result.output
    assert "alice" in result.output


def test_cli_query_invalid_output_format(server):
    """Test query command with invalid output format."""
    result = runner.invoke(
        cli,
        [
            "query",
            "SELECT * FROM users",
            "-h",
            "127.0.0.1",
            "-p",
            "18815",
            "-o",
            "invalid",
        ],
    )

    assert result.exit_code == 1
    assert "Unknown output format" in result.output


def test_cli_tables_with_server(server):
    """Test tables command against running server."""
    result = runner.invoke(
        cli,
        ["tables", "-h", "127.0.0.1", "-p", "18815"],
    )

    assert result.exit_code == 0
    assert "users" in result.output
