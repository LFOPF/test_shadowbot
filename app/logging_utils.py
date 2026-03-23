from __future__ import annotations

import logging
from collections import deque

log_buffer: deque[str] = deque(maxlen=100)


class LogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        log_buffer.append(self.format(record))


def configure_logging() -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    root = logging.getLogger()
    if not any(isinstance(handler, LogHandler) for handler in root.handlers):
        root.addHandler(LogHandler())
    return logging.getLogger("shadowbot")
