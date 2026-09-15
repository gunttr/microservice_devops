from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
import os

OTEL_SERVICE_NAME: str = os.getenv("OTEL_SERVICE_NAME", "notification_service")
# следующие переменные автоматически подтянуться sdk, если не передавать параметры
# OTEL_EXPORTER_OLTP_ENDPOINT=http://localhost:4318
# OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="http://localhost:4318/v1/metrics"
# OTEL_EXPORTER_OTLP_TIMEOUT=10000 (10s)
# OTEL_EXPORTER_OTLP_METRICS_TIMEOUT=10000 (10s)

resource = Resource.create(attributes={
    "service.name": OTEL_SERVICE_NAME
})

reader = PeriodicExportingMetricReader(OTLPMetricExporter())
meterProvider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meterProvider)

meter = metrics.get_meter(name=f"{OTEL_SERVICE_NAME}_meter")

notifications_sent_counter = meter.create_counter(
    name="notifications_sent_total",
    description="Total number of sent notifications"
)

successfully_sent_notifications_counter = meter.create_counter(
    name="successfully_sent_notifications_total",
    description="Total number of successfully sent notifications"
)

failed_to_send_notifications_counter = meter.create_counter(
    name="failed_to_send_notifications_total",
    description="Total number of failed to send notifications"
)
