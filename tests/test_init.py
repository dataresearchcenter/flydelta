from flydelta import Client, Server, __version__, serve


def test_version():
    assert __version__ == "0.0.0"


def test_exports():
    assert Client is not None
    assert Server is not None
    assert serve is not None
