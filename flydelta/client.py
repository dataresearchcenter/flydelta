"""Flight client for connecting to flydelta."""

from typing import Generator

import pyarrow as pa
import pyarrow.flight as flight


class Client:
    """Client for querying flydelta server."""

    def __init__(self, location: str = "grpc://localhost:8815"):
        self.location = location
        self._client = flight.connect(location)

    def stream_query(self, sql: str) -> Generator[pa.RecordBatch, None, None]:
        """Stream query results as record batches (memory efficient)."""
        descriptor = flight.FlightDescriptor.for_command(sql.encode("utf-8"))
        info = self._client.get_flight_info(descriptor)

        for endpoint in info.endpoints:
            reader = self._client.do_get(endpoint.ticket)
            for batch in reader:
                yield batch.data

    def query(self, sql: str) -> pa.Table:
        """Execute a SQL query and return results as Arrow table."""
        batches = list(self.stream_query(sql))
        if not batches:
            return pa.table({})
        return pa.Table.from_batches(batches)

    def list_tables(self) -> list[str]:
        """List available tables on the server."""
        tables = []
        for info in self._client.list_flights():
            if info.descriptor.path:
                tables.append(info.descriptor.path[0].decode("utf-8"))
        return tables

    def close(self) -> None:
        """Close the client connection."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
