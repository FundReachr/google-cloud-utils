import logging
import threading
from datetime import datetime


class GCSLogHandler(logging.Handler):
    """Buffered log handler that periodically flushes to GCS chunks.

    Flushes when the in-memory buffer reaches *max_lines* OR when the periodic
    timer fires (every *flush_interval* seconds), whichever comes first.
    Each flush writes an independent blob so memory stays bounded.

    Blob path: {prefix}/{session_ts}/chunk_{n:04d}.log
    """

    def __init__(self, bucket, prefix: str = "debug_logs", max_lines: int = 5000, flush_interval: float = 600.0):
        super().__init__(level=logging.DEBUG)
        self.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._max_lines = max_lines
        self._flush_interval = flush_interval
        self._buffer: list[str] = []
        self._chunk = 0
        self._lock = threading.Lock()
        self._session_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._timer: threading.Timer | None = None
        self._schedule()

    def _schedule(self):
        self._timer = threading.Timer(self._flush_interval, self._on_timer)
        self._timer.daemon = True
        self._timer.start()

    def _on_timer(self):
        self.flush()
        self._schedule()

    def emit(self, record: logging.LogRecord):
        try:
            line = self.format(record)
        except Exception:
            self.handleError(record)
            return
        with self._lock:
            self._buffer.append(line)
            if len(self._buffer) >= self._max_lines:
                self._flush_locked()

    def flush(self):
        with self._lock:
            self._flush_locked()

    def _flush_locked(self):
        if not self._buffer:
            return
        content = "\n".join(self._buffer) + "\n"
        blob_path = f"{self._prefix}/{self._session_ts}/chunk_{self._chunk:04d}.log"
        try:
            self._bucket.blob(blob_path).upload_from_string(content, content_type="text/plain")
        except Exception:
            pass  # never let logging errors crash the app
        self._buffer.clear()
        self._chunk += 1

    def close(self):
        if self._timer:
            self._timer.cancel()
        self.flush()
        super().close()
