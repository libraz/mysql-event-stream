"""Verify public API exports."""


def test_binlog_client_exported() -> None:
    from mysql_event_stream import BinlogClient
    assert BinlogClient is not None


def test_all_exports() -> None:
    import mysql_event_stream
    for name in mysql_event_stream.__all__:
        assert hasattr(mysql_event_stream, name), f"{name} not found in module"
