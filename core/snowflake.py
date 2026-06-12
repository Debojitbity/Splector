"""
Splector Snowflake ID Generator

Thread-safe unique ID generator for document_refs primary keys.
Based on the Twitter Snowflake algorithm.

Layout (64-bit integer):
  - 41 bits: millisecond timestamp (relative to custom epoch)
  - 10 bits: machine/worker ID (default 1)
  - 12 bits: sequence counter (per-millisecond)

This guarantees:
  - Chronological ordering
  - No collisions across threads or restarts
  - ~4096 IDs per millisecond per worker
"""

import threading
import time

# Custom epoch: 2024-01-01T00:00:00Z (milliseconds)
_EPOCH_MS = 1704067200000

_MACHINE_ID_BITS = 10
_SEQUENCE_BITS = 12

_MAX_MACHINE_ID = (1 << _MACHINE_ID_BITS) - 1  # 1023
_MAX_SEQUENCE = (1 << _SEQUENCE_BITS) - 1        # 4095

_MACHINE_ID_SHIFT = _SEQUENCE_BITS
_TIMESTAMP_SHIFT = _MACHINE_ID_BITS + _SEQUENCE_BITS


class SnowflakeGenerator:
    """Thread-safe Snowflake ID generator."""

    def __init__(self, machine_id: int = 1):
        if not (0 <= machine_id <= _MAX_MACHINE_ID):
            raise ValueError(
                f"machine_id must be between 0 and {_MAX_MACHINE_ID}"
            )
        self._machine_id = machine_id
        self._sequence = 0
        self._last_timestamp_ms = -1
        self._lock = threading.Lock()

    def _current_millis(self) -> int:
        return int(time.time() * 1000)

    def generate(self) -> int:
        """Generate a new unique Snowflake ID."""
        with self._lock:
            now_ms = self._current_millis()

            if now_ms == self._last_timestamp_ms:
                self._sequence = (self._sequence + 1) & _MAX_SEQUENCE
                if self._sequence == 0:
                    # Sequence exhausted — spin until next millisecond
                    while now_ms <= self._last_timestamp_ms:
                        now_ms = self._current_millis()
            else:
                self._sequence = 0

            self._last_timestamp_ms = now_ms

            snowflake_id = (
                ((now_ms - _EPOCH_MS) << _TIMESTAMP_SHIFT)
                | (self._machine_id << _MACHINE_ID_SHIFT)
                | self._sequence
            )
            return snowflake_id


# Module-level singleton for convenience
_default_generator = SnowflakeGenerator(machine_id=1)


def generate_id() -> str:
    """Generate a Snowflake ID as a string (safe for SQLite TEXT columns)."""
    return str(_default_generator.generate())
