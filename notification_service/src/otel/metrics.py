from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from src.otel.resource import OTEL_SERVICE_NAME, otel_resource

reader = PeriodicExportingMetricReader(OTLPMetricExporter())
meterProvider = MeterProvider(resource=otel_resource, metric_readers=[reader])
metrics.set_meter_provider(meterProvider)

meter = metrics.get_meter(name=f"{OTEL_SERVICE_NAME}_meter")

notifications_sent_total = meter.create_counter(
    name="notifications_sent_total",
    description="Total number of sent notifications"
)

notifications_succeeded_total = meter.create_counter(
    name="notifications_succeeded_total",
    description="Total number of successfully sent notifications"
)

notifications_failed_total = meter.create_counter(
    name="notifications_failed_total",
    description="Total number of failed to send notifications"
)
