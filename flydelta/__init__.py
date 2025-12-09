"""flydelta - A Flight SQL proxy for Delta Lake."""

from flydelta.client import Client
from flydelta.server import Server, serve

__version__ = "0.0.1"
__all__ = ["Client", "Server", "serve", "__version__"]
