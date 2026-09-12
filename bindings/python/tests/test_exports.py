"""Verify public API exports."""


def test_binlog_client_exported() -> None:
    from mysql_event_stream import BinlogClient

    assert BinlogClient is not None


def test_all_exports() -> None:
    import mysql_event_stream

    for name in mysql_event_stream.__all__:
        assert hasattr(mysql_event_stream, name), f"{name} not found in module"


def test_error_types_share_the_exported_base() -> None:
    from mysql_event_stream import ChecksumError, DecodeError, MesError, ParseError

    assert issubclass(MesError, RuntimeError)
    for category in (ParseError, DecodeError, ChecksumError):
        assert issubclass(category, MesError)


def test_connection_error_is_exported_in_the_os_error_category() -> None:
    from mysql_event_stream import MesConnectionError

    assert issubclass(MesConnectionError, ConnectionError)
    assert issubclass(MesConnectionError, OSError)


def test_ssl_mode_enum() -> None:
    from mysql_event_stream import SslMode

    assert SslMode.DISABLED.value == 0
    assert SslMode.PREFERRED.value == 1
    assert SslMode.REQUIRED.value == 2
    assert SslMode.VERIFY_CA.value == 3
    assert SslMode.VERIFY_IDENTITY.value == 4
    assert isinstance(SslMode.DISABLED, int)
