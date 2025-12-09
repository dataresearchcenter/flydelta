"""Flight SQL server for Delta Lake tables."""

from queue import Queue
from typing import Any, Generator

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from deltalake import DeltaTable


class Server(flight.FlightServerBase):
    """A Flight SQL server that queries Delta Lake tables via DuckDB."""

    def __init__(
        self,
        location: str = "grpc://0.0.0.0:8815",
        tables: dict[str, str] | None = None,
        pool_size: int = 10,
        batch_size: int = 100_000,
    ):
        super().__init__(location)
        self.location = location
        self.tables: dict[str, str] = tables or {}
        self.batch_size = batch_size

        # Load delta tables and cache schemas on boot
        self._delta_tables: dict[str, DeltaTable] = {}
        self._schemas: dict[str, pa.Schema] = {}
        for name, uri in self.tables.items():
            dt = DeltaTable(uri)
            self._delta_tables[name] = dt
            self._schemas[name] = dt.schema().to_pyarrow()

        # Create connection pool with tables pre-registered
        self._pool: Queue[duckdb.DuckDBPyConnection] = Queue(maxsize=pool_size)
        for _ in range(pool_size):
            conn = duckdb.connect(":memory:")
            for name, dt in self._delta_tables.items():
                conn.register(name, dt.to_pyarrow_dataset())
            self._pool.put(conn)

    def _get_schema(self, query: str) -> pa.Schema:
        """Get schema for a query without fetching data."""
        conn = self._pool.get()
        try:
            result = conn.execute(f"SELECT * FROM ({query}) LIMIT 0")
            return result.fetch_arrow_table().schema
        finally:
            self._pool.put(conn)

    def _stream_batches(
        self, query: str
    ) -> Generator[pa.RecordBatch, None, None]:
        """Stream query results as record batches."""
        conn = self._pool.get()
        try:
            reader = conn.execute(query).fetch_record_batch(self.batch_size)
            for batch in reader:
                yield batch
        finally:
            self._pool.put(conn)

    def do_get(
        self, context: flight.ServerCallContext, ticket: flight.Ticket
    ) -> flight.GeneratorStream:
        """Execute a query and stream results."""
        query = ticket.ticket.decode("utf-8")
        try:
            schema = self._get_schema(query)
            return flight.GeneratorStream(schema, self._stream_batches(query))
        except Exception as e:
            raise flight.FlightServerError(f"Query error: {e}")

    def get_flight_info(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
    ) -> flight.FlightInfo:
        """Get information about a query."""
        query = descriptor.command.decode("utf-8")

        try:
            schema = self._get_schema(query)
        except Exception as e:
            raise flight.FlightServerError(f"Query error: {e}")

        ticket = flight.Ticket(query.encode("utf-8"))
        endpoints = [flight.FlightEndpoint(ticket, [self.location])]

        return flight.FlightInfo(
            schema=schema,
            descriptor=descriptor,
            endpoints=endpoints,
            total_records=-1,
            total_bytes=-1,
        )

    def list_flights(
        self, context: flight.ServerCallContext, criteria: bytes
    ) -> Any:
        """List available tables."""
        for name, schema in self._schemas.items():
            descriptor = flight.FlightDescriptor.for_path(name)
            yield flight.FlightInfo(
                schema=schema,
                descriptor=descriptor,
                endpoints=[],
                total_records=-1,
                total_bytes=-1,
            )


def serve(
    host: str = "0.0.0.0",
    port: int = 8815,
    tables: dict[str, str] | None = None,
    pool_size: int = 10,
    batch_size: int = 100_000,
) -> None:
    """Start the flydelta server."""
    location = f"grpc://{host}:{port}"
    server = Server(
        location=location,
        tables=tables,
        pool_size=pool_size,
        batch_size=batch_size,
    )
    print(f"Starting flydelta on {location}")
    server.serve()
