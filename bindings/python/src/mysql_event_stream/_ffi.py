"""Low-level ctypes wrapper for libmes."""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import platform
import threading
import weakref
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
        ("names_resolved", ctypes.c_int32),
        ("source_sql", ctypes.c_char_p),
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
MES_ERR_GTID_PURGED = 405
MES_ERR_GTID_TAGGED_UNSUPPORTED = 406
MES_ABI_VERSION = 2

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


# NOTE: ctypes automatically inserts inter-field padding to match C ABI
# alignment rules (e.g., 6 bytes after uint16_t port to align the next
# pointer). Manual padding fields are NOT needed. A sizeof assertion in
# load_library() verifies layout correctness at import time.
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
        ("allow_public_key_retrieval", ctypes.c_int32),
        ("start_position_mode", ctypes.c_int32),
        ("binlog_file", ctypes.c_char_p),
        ("binlog_position", ctypes.c_uint64),
    ]


class MESPollResult(ctypes.Structure):
    """Maps to mes_poll_result_t."""

    _fields_ = [
        ("error", ctypes.c_int32),
        ("data", ctypes.POINTER(ctypes.c_uint8)),
        ("size", ctypes.c_size_t),
        ("is_heartbeat", ctypes.c_int32),
        ("checksum_enabled", ctypes.c_int32),
    ]


# The LP64 field positions the core publishes for the structs above, recorded
# the way their sizes are: a mirror has to reproduce them exactly, and the core
# fails its own build if either layout moves without these numbers moving too.
_CLIENT_CONFIG_OFFSETS = {
    "host": 0,
    "port": 8,
    "user": 16,
    "password": 24,
    "server_id": 32,
    "start_gtid": 40,
    "connect_timeout_s": 48,
    "read_timeout_s": 52,
    "ssl_mode": 56,
    "ssl_ca": 64,
    "ssl_cert": 72,
    "ssl_key": 80,
    "max_queue_size": 88,
    "allow_public_key_retrieval": 96,
    "start_position_mode": 100,
    "binlog_file": 104,
    "binlog_position": 112,
}
_POLL_RESULT_OFFSETS = {
    "error": 0,
    "data": 8,
    "size": 16,
    "is_heartbeat": 24,
    "checksum_enabled": 28,
}


# Names the library can carry inside a distribution, across platforms.
_LIB_NAMES = ("libmes.dylib", "libmes.so", "mes.dll")

# Development build directories, in the order every harness in the repository
# prefers them. Both export the client entry points: OpenSSL is a required
# dependency of the core, so no build of it omits them.
_BUILD_DIRS = ("build-client", "build")


def _platform_lib_name() -> str:
    """Return the library file name used on the running platform."""
    system = platform.system()
    if system == "Darwin":
        return "libmes.dylib"
    if system == "Windows":
        return "mes.dll"
    return "libmes.so"


def _source_tree_root(pkg_dir: Path) -> Path | None:
    """Return the repository root when the package is imported from a checkout.

    In a distribution the package directory is a plain site-packages entry. In
    the repository it sits at ``bindings/python/src/mysql_event_stream``, beside
    the binding's ``pyproject.toml``; that file is what tells the two apart, and
    it is also what makes the development build directories addressable.

    Args:
        pkg_dir: Directory this package is being imported from.

    Returns:
        The repository root, or None when this is not a source checkout.
    """
    binding_root = pkg_dir.parent.parent
    if pkg_dir.parent.name != "src" or not (binding_root / "pyproject.toml").exists():
        return None
    return binding_root.parent.parent


def _resolve_library_path(pkg_dir: Path, lib_name: str) -> Path | None:
    """Return the library that belongs to the environment the package lives in.

    A source checkout resolves to the development build. Building a wheel stages
    a copy of the library next to the package, and nothing refreshes that copy
    when the core is rebuilt, so preferring it inside a checkout would bind a
    superseded image while the test and example harnesses bind the fresh one.
    Outside a checkout the staged copy is the library the distribution ships and
    the only one there is, so it wins.

    Args:
        pkg_dir: Directory this package is being imported from.
        lib_name: Library file name used on the running platform.

    Returns:
        Path to the library to load, or None when no candidate exists.
    """
    source_root = _source_tree_root(pkg_dir)
    if source_root is not None:
        for build_dir in _BUILD_DIRS:
            candidate = source_root / build_dir / "core" / lib_name
            if candidate.exists():
                return candidate
        return None

    for name in _LIB_NAMES:
        candidate = pkg_dir / name
        if candidate.exists():
            return candidate
    return None


def _find_library() -> str:
    """Find the libmes shared library.

    Search order:
        1. ``MES_LIB_PATH``, which must name an existing file
        2. The development build, when imported from a source checkout
        3. The library shipped next to the package, otherwise
        4. System library path

    An ``MES_LIB_PATH`` that does not exist is an error rather than a fallback:
    resolving to a different library than the caller asked for would load a
    second image of libmes into the process, and the two images share no state.

    Returns:
        Path to the shared library.

    Raises:
        OSError: If ``MES_LIB_PATH`` does not exist, or no library is found.
    """
    env_path = os.environ.get("MES_LIB_PATH")
    if env_path:
        if not Path(env_path).exists():
            raise OSError(f"MES_LIB_PATH is set to {env_path}, which does not exist.")
        return env_path

    pkg_dir = Path(__file__).parent
    lib_name = _platform_lib_name()
    resolved = _resolve_library_path(pkg_dir, lib_name)
    if resolved is not None:
        return str(resolved)

    path = ctypes.util.find_library("mes")
    if path:
        return path

    source_root = _source_tree_root(pkg_dir)
    if source_root is not None:
        checked = ", ".join(str(source_root / d / "core" / lib_name) for d in _BUILD_DIRS)
        raise OSError(
            f"libmes shared library not found. Checked: {checked}. Build it with "
            "'cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build "
            "--parallel', or set MES_LIB_PATH to an existing library. A copy staged "
            "next to the package for wheel building is not used from a source "
            "checkout, because rebuilding the core does not refresh it."
        )
    raise OSError(
        f"libmes shared library not found next to the installed package in {pkg_dir}. "
        "The distribution is incomplete; set MES_LIB_PATH to an existing library."
    )


def _verify_struct_sizes(lib: ctypes.CDLL) -> None:
    """Verify ctypes struct sizes match the C ABI layout.

    The event and column structs are validated against the authoritative
    ``sizeof`` reported by the loaded library (``mes_sizeof_event`` /
    ``mes_sizeof_column``), so the check is exact and portable across
    pointer widths and C-side struct packing. A mismatch means the ctypes
    layout is stale and every C call using these structs would corrupt
    memory, so it is a hard error.

    Args:
        lib: The loaded shared library, with introspection signatures set.
    """
    event_size = ctypes.sizeof(MESEvent)
    expected_event = lib.mes_sizeof_event()
    if event_size != expected_event:
        raise RuntimeError(
            f"MESEvent size mismatch: ctypes={event_size}, "
            f"libmes={expected_event}. ABI incompatibility detected."
        )

    column_size = ctypes.sizeof(MESColumn)
    expected_column = lib.mes_sizeof_column()
    if column_size != expected_column:
        raise RuntimeError(
            f"MESColumn size mismatch: ctypes={column_size}, "
            f"libmes={expected_column}. ABI incompatibility detected."
        )

    # The two structs a caller allocates rather than receives have no C-side
    # sizeof helper, because a mirror that got the layout wrong would already
    # have corrupted memory by the time it could call one. The library instead
    # pins their size and every field offset with assertions it compiles on
    # every build, so these numbers are checked on both sides of the boundary
    # rather than only here. Both are LP64 layouts; on a narrower platform the
    # sizes differ and the layout is exercised at call time instead.
    #
    # The field offsets are checked alongside the size because a size alone
    # misses a field that lands in what used to be tail padding: the total is
    # unchanged and a mirror without that field still reads the right bytes for
    # every field before it, while the new one is silently absent.
    import struct as _struct

    if _struct.calcsize("P") == 8:
        for name, mirror, expected, offsets in (
            ("MESClientConfig", MESClientConfig, 120, _CLIENT_CONFIG_OFFSETS),
            ("MESPollResult", MESPollResult, 32, _POLL_RESULT_OFFSETS),
        ):
            actual = ctypes.sizeof(mirror)
            if actual != expected:
                raise RuntimeError(
                    f"{name} size mismatch: got {actual}, expected {expected}. "
                    "ABI incompatibility detected."
                )
            mirrored = {field[0]: getattr(mirror, field[0]).offset for field in mirror._fields_}
            if mirrored != offsets:
                raise RuntimeError(
                    f"{name} field offsets {mirrored} do not match the published "
                    f"layout {offsets}. ABI incompatibility detected."
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

    lib.mes_version.restype = ctypes.c_char_p
    lib.mes_version.argtypes = []
    lib.mes_abi_version.restype = ctypes.c_uint32
    lib.mes_abi_version.argtypes = []
    abi_version = lib.mes_abi_version()
    if abi_version != MES_ABI_VERSION:
        raw_version = lib.mes_version()
        version = raw_version.decode("utf-8", errors="replace") if raw_version else "unknown"
        raise RuntimeError(
            f"libmes ABI {abi_version} (version {version}) is incompatible with "
            f"this binding (requires ABI {MES_ABI_VERSION})"
        )

    # mes_create
    lib.mes_create.restype = ctypes.c_void_p
    lib.mes_create.argtypes = []

    # mes_destroy
    lib.mes_destroy.restype = None
    lib.mes_destroy.argtypes = [ctypes.c_void_p]

    # mes_error_string
    lib.mes_error_string.restype = ctypes.c_char_p
    lib.mes_error_string.argtypes = [ctypes.c_int32]

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

    # mes_set_max_queue_bytes / mes_get_max_queue_bytes
    lib.mes_set_max_queue_bytes.restype = ctypes.c_int32
    lib.mes_set_max_queue_bytes.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    lib.mes_get_max_queue_bytes.restype = ctypes.c_size_t
    lib.mes_get_max_queue_bytes.argtypes = [ctypes.c_void_p]

    # mes_set_max_event_size
    lib.mes_set_max_event_size.restype = ctypes.c_int32
    lib.mes_set_max_event_size.argtypes = [ctypes.c_void_p, ctypes.c_uint32]

    # mes_sizeof_event / mes_sizeof_column (ABI introspection)
    lib.mes_sizeof_event.restype = ctypes.c_size_t
    lib.mes_sizeof_event.argtypes = []
    lib.mes_sizeof_column.restype = ctypes.c_size_t
    lib.mes_sizeof_column.argtypes = []

    # mes_get_max_event_size
    lib.mes_get_max_event_size.restype = ctypes.c_uint32
    lib.mes_get_max_event_size.argtypes = [ctypes.c_void_p]

    # mes_set_checksum_enabled
    lib.mes_set_checksum_enabled.restype = ctypes.c_int32
    lib.mes_set_checksum_enabled.argtypes = [ctypes.c_void_p, ctypes.c_int]

    # mes_set_trailer_pre_verified / mes_get_trailer_pre_verified
    lib.mes_set_trailer_pre_verified.restype = ctypes.c_int32
    lib.mes_set_trailer_pre_verified.argtypes = [ctypes.c_void_p, ctypes.c_int]
    lib.mes_get_trailer_pre_verified.restype = ctypes.c_int32
    lib.mes_get_trailer_pre_verified.argtypes = [ctypes.c_void_p]

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

    # Verify struct layout matches C ABI. If this fails, the ctypes struct
    # fields are misaligned and all C calls using this struct will corrupt memory.
    _verify_struct_sizes(lib)

    return lib


_loaded_lib: ctypes.CDLL | None = None
_lib_lock = threading.Lock()


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
        with _lib_lock:
            if _loaded_lib is None:
                _loaded_lib = load_library()
    return _loaded_lib


_CLIENT_SYMBOLS = [
    "mes_client_create",
    "mes_client_destroy",
    "mes_client_connect",
    "mes_client_start",
    "mes_client_poll",
    "mes_client_poll_batch",
    "mes_client_stop",
    "mes_client_disconnect",
    "mes_client_is_connected",
    "mes_client_is_streaming",
    "mes_client_flavor",
    "mes_client_last_error",
    "mes_client_current_gtid",
    "mes_client_checksum_enabled",
    "mes_client_set_max_event_size",
    "mes_client_get_max_event_size",
    "mes_client_set_max_queue_bytes",
    "mes_client_get_max_queue_bytes",
    "mes_client_queued_bytes",
    "mes_client_crc_errors",
    "mes_engine_set_metadata_conn",
]


_client_lock = threading.Lock()
# Track which CDLL instances have already had their client-API signatures
# configured. Using a WeakValueDictionary keyed by id() lets different
# ctypes.CDLL objects (e.g. for different lib_path in tests) each get
# configured independently without leaking references once the CDLL is
# garbage-collected.
#
# Note: a previous implementation used a single module-level
# _client_configured boolean, which incorrectly treated the second lib
# passed to load_client_library as already configured. That broke tests
# that loaded two different libmes builds side-by-side.
_client_configured_libs: weakref.WeakValueDictionary[int, ctypes.CDLL] = (
    weakref.WeakValueDictionary()
)


def load_client_library(lib: ctypes.CDLL) -> bool:
    """Configure BinlogClient function signatures if available.

    All required symbols are verified to exist before any signature is
    configured, preventing partial configuration on incomplete builds.
    Thread-safe: uses double-checked locking to prevent concurrent
    partial configuration.  Configuration is tracked per-CDLL so that
    multiple independent libraries (different ``lib_path``) each get
    properly configured.

    Args:
        lib: Already-loaded ctypes.CDLL instance.

    Returns:
        True if client functions are available, False otherwise.
    """
    lib_id = id(lib)
    if lib_id in _client_configured_libs:
        return True
    with _client_lock:
        if lib_id in _client_configured_libs:
            return True
        if not all(hasattr(lib, sym) for sym in _CLIENT_SYMBOLS):
            return False

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

        lib.mes_client_poll_batch.restype = ctypes.c_int32
        lib.mes_client_poll_batch.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(MESPollResult),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_size_t),
        ]

        lib.mes_client_stop.restype = None
        lib.mes_client_stop.argtypes = [ctypes.c_void_p]

        lib.mes_client_disconnect.restype = None
        lib.mes_client_disconnect.argtypes = [ctypes.c_void_p]

        lib.mes_client_is_connected.restype = ctypes.c_int32
        lib.mes_client_is_connected.argtypes = [ctypes.c_void_p]

        lib.mes_client_is_streaming.restype = ctypes.c_int32
        lib.mes_client_is_streaming.argtypes = [ctypes.c_void_p]

        lib.mes_client_flavor.restype = ctypes.c_int32
        lib.mes_client_flavor.argtypes = [ctypes.c_void_p]

        lib.mes_client_last_error.restype = ctypes.c_char_p
        lib.mes_client_last_error.argtypes = [ctypes.c_void_p]

        lib.mes_client_set_max_event_size.restype = ctypes.c_int32
        lib.mes_client_set_max_event_size.argtypes = [ctypes.c_void_p, ctypes.c_uint32]

        lib.mes_client_get_max_event_size.restype = ctypes.c_uint32
        lib.mes_client_get_max_event_size.argtypes = [ctypes.c_void_p]

        lib.mes_client_set_max_queue_bytes.restype = ctypes.c_int32
        lib.mes_client_set_max_queue_bytes.argtypes = [ctypes.c_void_p, ctypes.c_size_t]

        lib.mes_client_get_max_queue_bytes.restype = ctypes.c_size_t
        lib.mes_client_get_max_queue_bytes.argtypes = [ctypes.c_void_p]

        lib.mes_client_queued_bytes.restype = ctypes.c_size_t
        lib.mes_client_queued_bytes.argtypes = [ctypes.c_void_p]

        lib.mes_client_crc_errors.restype = ctypes.c_uint64
        lib.mes_client_crc_errors.argtypes = [ctypes.c_void_p]

        lib.mes_client_current_gtid.restype = ctypes.c_char_p
        lib.mes_client_current_gtid.argtypes = [ctypes.c_void_p]

        lib.mes_client_checksum_enabled.restype = ctypes.c_int32
        lib.mes_client_checksum_enabled.argtypes = [ctypes.c_void_p]

        lib.mes_engine_set_metadata_conn.restype = ctypes.c_int32
        lib.mes_engine_set_metadata_conn.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(MESClientConfig),
        ]

        _client_configured_libs[lib_id] = lib
        return True
