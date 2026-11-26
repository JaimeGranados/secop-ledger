"""Keep Windows from going to sleep while the collector downloads.

Why: on 2026-11-25 the desktop went to sleep twice in the middle of the download. While it slept,
the source refreshed the dataset and gave every row a new `:id`, so the keyset read continued in a
different id space and read about 750,000 rows twice (the count check caught it; the day was lost).

`SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)` tells Windows "this process needs the
system" until we clear it, so the idle sleep timer is ignored only while the run is going. The
power settings of the PC do not change. It does not stop someone choosing Sleep in the Start menu.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager

log = logging.getLogger(__name__)

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001


def _windows_setter() -> Callable[[int], int] | None:
    if sys.platform != "win32":
        return None  # Linux CI, Docker: nothing to do
    import ctypes

    return ctypes.windll.kernel32.SetThreadExecutionState


@contextmanager
def keep_awake(set_state: Callable[[int], int] | None = None) -> Iterator[None]:
    """Block idle sleep inside the `with` block. Tests pass a fake `set_state`."""
    setter = set_state or _windows_setter()
    if setter is None:
        yield
        return
    if setter(ES_CONTINUOUS | ES_SYSTEM_REQUIRED) == 0:
        log.warning("could not block sleep; the run may stop if the PC sleeps")
    try:
        yield
    finally:
        setter(ES_CONTINUOUS)  # back to normal: the PC may sleep again
