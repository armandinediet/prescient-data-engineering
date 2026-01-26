import logging
import sys
import uuid

_RUN_ID = None
_prev_factory = None

def setup_logging(run_id: str | None = None, level: str = "INFO") -> str:
    global _RUN_ID, _prev_factory
    _RUN_ID = run_id or str(uuid.uuid4())

    # Handler + formatter
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s run_id=%(run_id)s %(name)s - %(message)s"
    ))

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers[:] = [handler]

    # Inject run_id into *every* LogRecord (all loggers)
    if _prev_factory is None:
        _prev_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = _prev_factory(*args, **kwargs)
        if not hasattr(record, "run_id"):
            record.run_id = _RUN_ID
        return record

    logging.setLogRecordFactory(record_factory)
    return _RUN_ID
