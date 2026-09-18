import logging
from typing import Literal, Mapping, Any
import json
from src.otel.resource import otel_resource
from opentelemetry._logs import set_logger_provider, get_logger
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
import os


class JSONFormater(logging.Formatter):
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


class OTelJSONHandler(logging.Handler):
    def __init__(self, logger_provider):
        super().__init__()

        self.otel_logger = get_logger(
            "root",
            logger_provider=logger_provider,
        )

    def emit(self, record: logging.LogRecord):
        try:
            body = self.format(record)

            self.otel_logger.emit(
                body=body,
                severity_text=record.levelname,
                severity_number=record.levelno,
            )
        except Exception:
            self.handleError(record)


# Настройка логера
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", logging.INFO))

# настройка otlp экспортера
# OTEL_EXPORTER_OLTP_ENDPOINT=http://localhost:4318
# OTEL_EXPORTER_OTLP_LOGS_ENDPOINT="http://localhost:4318/v1/logs"
# OTEL_EXPORTER_OTLP_TIMEOUT=10000 (10s)
# OTEL_EXPORTER_OTLP_LOGS_TIMEOUT=10000 (10s)
exporter = OTLPLogExporter()

# Настройка logger провайдера
logger_provider = LoggerProvider(otel_resource)
logger_provider.add_log_record_processor(
    SimpleLogRecordProcessor(exporter)
)
set_logger_provider(logger_provider)

# используется мой json_formater и для вывода в консоль, и для отправки в otel-collector
json_formater = JSONFormater()

# консольный хендлер
console_handler = logging.StreamHandler()
console_handler.setFormatter(json_formater)
logger.addHandler(console_handler)

# хендлер в otel-collector
otel_handler = OTelJSONHandler(logger_provider)
otel_handler.setFormatter(json_formater)
logger.addHandler(otel_handler)
