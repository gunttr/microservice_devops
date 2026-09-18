import logging
from typing import Literal, Mapping, Any
import json

class JSONLogger(logging.Formatter):
    _STANDARD_LOG_RECORD_ATTRS = set(
        vars(logging.LogRecord(
            name="",
            level=0,
            pathname="",
            lineno=0,
            msg="",
            args=(),
            exc_info=None,
        ))
    )

    def __init__(
        self, 
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Literal['%'] | Literal['{'] | Literal['$'] = "%",
        validate: bool = True,
        *,
        defaults: Mapping[str, Any] | None = None
    ) -> None:
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)
        

    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "service_name": "order",
        }

        # extra, например, для order_id или username полей
        extra = {
            key: value
            for key, value in record.__dict__.items()
            if key not in self._STANDARD_LOG_RECORD_ATTRS
        }

        if extra:
            log_record["extra"] = extra

        return json.dumps(log_record)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()

formatter = JSONLogger()
handler.setFormatter(formatter)
logger.addHandler(handler)