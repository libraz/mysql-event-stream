"""Low-level ctypes wrapper for libmes."""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import platform
from pathlib import Path


class MESColumn(ctypes.Structure):
    """Maps to mes_column_t."""

    _fields_ = [
        ("type", ctypes.c_int32),
        ("int_val", ctypes.c_int64),
        ("double_val", ctypes.c_double),
        # Use c_void_p instead of c_char_p: c_char_p stops at the first null
        # byte (\x00), which silently truncates BLOB/BINARY data that may
        # contain embedded nulls. Slice manually using str_len instead.
        ("str_data", ctypes.c_void_p),
        ("str_len", ctypes.c_uint32),
        ("col_name", ctypes.c_char_p),
    ]


class MESEvent(ctypes.Structure):
    """Maps to mes_event_t."""

    _fields_ = [
        ("type", ctypes.c_int32),
        ("database", ctypes.c_char_p),
        ("table", ctypes.c_char_p),
        ("before_columns", ctypes.POINTER(MESColumn)),
        ("before_count", ctypes.c_uint32),
        ("after_columns", ctypes.POINTER(MESColumn)),
        ("after_count", ctypes.c_uint32),
        ("timestamp", ctypes.c_uint32),
        ("binlog_file", ctypes.c_char_p),
        ("binlog_offset", ctypes.c_uint64),
    ]


# Error codes
MES_OK = 0
MES_ERR_NULL_ARG = 1
MES_ERR_INVALID_ARG = 2
MES_ERR_INTERNAL = 99
MES_ERR_PARSE = 100
MES_ERR_CHECKSUM = 101
MES_ERR_DECODE = 200
MES_ERR_DECODE_COLUMN = 201
MES_ERR_DECODE_ROW = 202
MES_ERR_NO_EVENT = 300
MES_ERR_QUEUE_FULL = 301
MES_ERR_CONNECT = 400
MES_ERR_AUTH = 401
MES_ERR_VALIDATION = 402
MES_ERR_STREAM = 403
MES_ERR_DISCONNECTED = 404

# Log levels
MES_LOG_ERROR = 0
MES_LOG_WARN = 1
MES_LOG_INFO = 2
MES_LOG_DEBUG = 3

# Log callback function type: void (*)(int level, const char* message, void* userdata)
# In the Python binding, userdata is always passed as None (null pointer).
MES_LOG_CALLBACK = ctypes.CFUNCTYPE(None, ctypes.c_int32, ctypes.c_char_p, ctypes.c_void_p)

# Column types
MES_COL_NULL = 0
MES_COL_INT = 1
MES_COL_DOUBLE = 2
MES_COL_STRING = 3
MES_COL_BYTES = 4


class MESClientConfig(ctypes.Structure):
    """Maps to mes_client_config_t."""

    _fields_ = [
        ("host", ctypes.c_char_p),
        ("port", ctypes.c_uint16),
        ("user", ctypes.c_char_p),
        ("password", ctypes.c_char_p),
        ("server_id", ctypes.c_uint32),
        ("start_gtid", ctypes.c_char_p),
        ("connect_timeout_s", ctypes.c_uint32),
        ("read_timeout_s", ctypes.c_uint32),
        ("ssl_mode", ctypes.c_uint32),
        ("ssl_ca", ctypes.c_char_p),
        ("ssl_cert", ctypes.c_char_p),
        ("ssl_key", ctypes.c_char_p),
        ("max_queue_size", ctypes.c_size_t),
    ]


class MESPollResult(ctypes.Structure):
    """Maps to mes_poll_result_t."""

    _fields_ = [
        ("error", ctypes.c_int32),
        ("data", ctypes.POINTER(ctypes.c_uint8)),
        ("size", ctypes.c_size_t),
        ("is_heartbeat", ctypes.c_int32),
    ]


def _find_library() -> str:
    """Find the libmes shared library.

    Search order:
        1. MES_LIB_PATH environment variable
        2. Package-adjacent (wheel distribution)
        3. Build directory (development)
        4. System library path
    """
    env_path = os.environ.get("MES_LIB_PATH")
    if env_path and Path(env_path).exists():
        return env_path

    pkg_dir = Path(__file__).parent
    for name in ("libmes.dylib", "libmes.so", "mes.dll"):
        candidate = pkg_dir / name
        if candidate.exists():
            return str(candidate)

    # Dev build dir: src/mysql_event_stream/ -> bindings/python -> bindings -> project root
    project_root = pkg_dir.parent.parent.parent.parent
    lib_name = "libmes.dylib" if platform.system() == "Darwin" else "libmes.so"
    build_path = project_root / "build" / "core" / lib_name
    if build_path.exists():
        return str(build_path)

    path = ctypes.util.find_library("mes")
    if path:
        return path

    raise OSError(
        "libmes shared library not found. "
        "Set MES_LIB_PATH or build with: cmake --build build --parallel"
    )


def load_library(lib_path: str | None = None) -> ctypes.CDLL:
    """Load libmes and configure function signatures.

    Args:
        lib_path: Explicit path to the shared library. If None, searches
            standard locations.

    Returns:
        Loaded ctypes.CDLL with typed function signatures.

    Raises:
        OSError: If the library cannot be found or loaded.
    """
    path = lib_path or _find_library()
    lib = ctypes.CDLL(path)

    # mes_create
    lib.mes_create.restype = ctypes.c_void_p
    lib.mes_create.argtypes = []

    # mes_destroy
    lib.mes_destroy.restype = None
    lib.mes_destroy.argtypes = [ctypes.c_void_p]

    # mes_feed
    lib.mes_feed.restype = ctypes.c_int32
    lib.mes_feed.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]

    # mes_next_event
    lib.mes_next_event.restype = ctypes.c_int32
    lib.mes_next_event.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.POINTER(MESEvent)),
    ]

    # mes_has_events
    lib.mes_has_events.restype = ctypes.c_int32
    lib.mes_has_events.argtypes = [ctypes.c_void_p]

    # mes_get_position
    lib.mes_get_position.restype = ctypes.c_int32
    lib.mes_get_position.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.POINTER(ctypes.c_uint64),
    ]

    # mes_reset
    lib.mes_reset.restype = ctypes.c_int32
    lib.mes_reset.argtypes = [ctypes.c_void_p]

    # mes_set_max_queue_size
    lib.mes_set_max_queue_size.restype = ctypes.c_int32
    lib.mes_set_max_queue_size.argtypes = [ctypes.c_void_p, ctypes.c_size_t]

    # mes_set_log_callback
    lib.mes_set_log_callback.restype = None
    lib.mes_set_log_callback.argtypes = [MES_LOG_CALLBACK, ctypes.c_int32, ctypes.c_void_p]

    # mes_set_include_databases
    lib.mes_set_include_databases.restype = ctypes.c_int32
    lib.mes_set_include_databases.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.c_size_t,
    ]

    # mes_set_include_tables
    lib.mes_set_include_tables.restype = ctypes.c_int32
    lib.mes_set_include_tables.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.c_size_t,
    ]

    # mes_set_exclude_tables
    lib.mes_set_exclude_tables.restype = ctypes.c_int32
    lib.mes_set_exclude_tables.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.c_size_t,
    ]

    return lib


_loaded_lib: ctypes.CDLL | None = None


def get_library(lib_path: str | None = None) -> ctypes.CDLL:
    """Return a cached library instance, loading it on first call.

    When ``lib_path`` is provided, a fresh library is loaded (bypassing
    the cache) to honour the caller's explicit path.  When ``lib_path``
    is None, the module-level singleton is reused.

    Args:
        lib_path: Explicit path to the shared library.  If None, reuses
            the cached instance (or searches standard locations on first
            call).

    Returns:
        Loaded ctypes.CDLL with typed function signatures.
    """
    global _loaded_lib
    if lib_path is not None:
        return load_library(lib_path)
    if _loaded_lib is None:
        _loaded_lib = load_library()
    return _loaded_lib


def load_client_library(lib: ctypes.CDLL) -> bool:
    """Configure BinlogClient function signatures if available.

    Args:
        lib: Already-loaded ctypes.CDLL instance.

    Returns:
        True if client functions are available, False otherwise.
    """
    try:
        lib.mes_client_create.restype = ctypes.c_void_p
        lib.mes_client_create.argtypes = []

        lib.mes_client_destroy.restype = None
        lib.mes_client_destroy.argtypes = [ctypes.c_void_p]

        lib.mes_client_connect.restype = ctypes.c_int32
        lib.mes_client_connect.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(MESClientConfig),
        ]

        lib.mes_client_start.restype = ctypes.c_int32
        lib.mes_client_start.argtypes = [ctypes.c_void_p]

        lib.mes_client_poll.restype = MESPollResult
        lib.mes_client_poll.argtypes = [ctypes.c_void_p]

        lib.mes_client_stop.restype = None
        lib.mes_client_stop.argtypes = [ctypes.c_void_p]

        lib.mes_client_disconnect.restype = None
        lib.mes_client_disconnect.argtypes = [ctypes.c_void_p]

        lib.mes_client_is_connected.restype = ctypes.c_int32
        lib.mes_client_is_connected.argtypes = [ctypes.c_void_p]

        lib.mes_client_last_error.restype = ctypes.c_char_p
        lib.mes_client_last_error.argtypes = [ctypes.c_void_p]

        lib.mes_client_current_gtid.restype = ctypes.c_char_p
        lib.mes_client_current_gtid.argtypes = [ctypes.c_void_p]

        lib.mes_engine_set_metadata_conn.restype = ctypes.c_int32
        lib.mes_engine_set_metadata_conn.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(MESClientConfig),
        ]

        return True
    except AttributeError:
        return False
