[![flydelta on pypi](https://img.shields.io/pypi/v/flydelta)](https://pypi.org/project/flydelta/)
[![Python test and package](https://github.com/dataresearchcenter/flydelta/actions/workflows/python.yml/badge.svg)](https://github.com/dataresearchcenter/flydelta/actions/workflows/python.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](https://coveralls.io/repos/github/dataresearchcenter/flydelta/badge.svg?branch=main)](https://coveralls.io/github/dataresearchcenter/flydelta?branch=main)
[![AGPLv3+ License](https://img.shields.io/pypi/l/flydelta)](./LICENSE)

# flydelta

A Flight SQL proxy for [Delta Lake](https://delta.io/). Query Delta tables via [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) with efficient streaming and predicate pushdown.

`flydelta` is read-only on existing data and has no authentication logic to keep it very simple.

## Why?

When a Delta Lake storage backend (on S3, disk, etc.) is queried by multiple client applications, having each client read Parquet files from the source storage directly is inefficient due to network traffic, even when using predicate pushdown.

`flydelta` solves this by acting as a query proxy deployed close to the data:

![Architecture](./docs/architecture.svg)

## Installation

```bash
pip install flydelta
```

## Usage

### Server

Start a `flydelta` server with Delta tables:

```bash
flydelta serve -t users=s3://bucket/users -t orders=/data/orders
```

Options:

```bash
flydelta serve \
  --host 0.0.0.0 \
  --port 8815 \
  --table users=s3://bucket/users \
  --table orders=/data/orders \
  --pool-size 20 \
  --batch-size 100000
```

### Docker

```bash
docker build -t flydelta .
docker run -p 8815:8815 flydelta -t users=/data/users -t orders=/data/orders
```

### Python Client

```python
from flydelta import Client

with Client("grpc://localhost:8815") as client:
    # Query to Arrow table
    table = client.query("SELECT * FROM users WHERE active = true")

    # Convert to pandas DataFrame
    df = table.to_pandas()

    # List available tables
    tables = client.list_tables()
```

### Streaming Large Results

For memory-efficient processing of large result sets:

```python
from flydelta import Client

with Client("grpc://localhost:8815") as client:
    for batch in client.stream_query("SELECT * FROM huge_table"):
        # Process each batch (default 100k rows)
        for row in batch.to_pylist():
            process(row)

        # Or process columnar (faster)
        ids = batch.column('id')
        values = batch.column('value')
```

### CLI Client

```bash
# Query with table output
flydelta query "SELECT * FROM users LIMIT 10"

# Query with JSON output
flydelta query "SELECT * FROM users" -o json

# Query with CSV output
flydelta query "SELECT * FROM users" -o csv

# List tables
flydelta tables
```

## Architecture

`flydelta` uses:

- **[delta-rs](https://github.com/delta-io/delta-rs)**: Rust-based Delta Lake implementation (no Spark needed)
- **[DuckDB](https://duckdb.org/)**: Fast SQL execution with predicate pushdown
- **[Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)**: Efficient gRPC-based data transfer

On startup, flydelta:

1. Loads Delta table metadata
2. Creates a connection pool with tables pre-registered
3. Caches schemas for fast query planning

Queries are executed via DuckDB and streamed back as [Arrow record batches](https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html#pyarrow.RecordBatch).

## Development

This package uses [poetry](https://python-poetry.org/) for packaging and dependencies management.

```bash
# Clone and install
git clone https://github.com/dataresearchcenter/flydelta.git
cd flydelta
poetry install --with dev

# Setup pre-commit hooks
poetry run pre-commit install

# Run tests
make test

# Run linting
make lint
```

## Disclaimer

Despite the name suggesting otherwise, `flydelta` has no affiliation with _Delta Air Lines_. We cannot help you book flights, upgrade your SkyMiles status, or locate your lost luggage. Actually, please stop flying at all if possible. 🌱

## License

`flydelta` is licensed under the AGPLv3 or later license. See [LICENSE](./LICENSE).
