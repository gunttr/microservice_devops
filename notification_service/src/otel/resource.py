from opentelemetry.sdk.resources import Resource
import os

OTEL_SERVICE_NAME: str = os.getenv("OTEL_SERVICE_NAME", "notification_service")
# следующие переменные автоматически подтянуться sdk, если не передавать параметры
# OTEL_EXPORTER_OLTP_ENDPOINT=http://localhost:4318
# OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="http://localhost:4318/v1/metrics"
# OTEL_EXPORTER_OTLP_TIMEOUT=10000 (10s)
# OTEL_EXPORTER_OTLP_METRICS_TIMEOUT=10000 (10s)
otel_resource = Resource.create(attributes={
    "service.name": OTEL_SERVICE_NAME
})