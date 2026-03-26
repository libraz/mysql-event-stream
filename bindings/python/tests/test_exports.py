"""Verify public API exports."""


def test_binlog_client_exported() -> None:
    from mysql_event_stream import BinlogClient
    assert BinlogClient is not None


def test_all_exports() -> None:
    import mysql_event_stream
    for name in mysql_event_stream.__all__:
        assert hasattr(mysql_event_stream, name), f"{name} not found in module"


def test_ssl_mode_enum() -> None:
    from mysql_event_stream import SslMode
    assert SslMode.DISABLED == 0
    assert SslMode.PREFERRED == 1
    assert SslMode.REQUIRED == 2
    assert SslMode.VERIFY_CA == 3
    assert SslMode.VERIFY_IDENTITY == 4
    assert isinstance(SslMode.DISABLED, int)
