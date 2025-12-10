"""flydelta - A Flight SQL proxy for Delta Lake."""

from flydelta.client import Client
from flydelta.server import SERVER_DEPS_AVAILABLE

__version__ = "0.0.1"
__all__ = ["Client", "__version__"]

if SERVER_DEPS_AVAILABLE:
    from flydelta.server import Server, serve  # noqa: F401

    __all__.extend(["Server", "serve"])
