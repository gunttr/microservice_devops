from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from src.otel.resource import OTEL_SERVICE_NAME, otel_resource

# следующие переменные окружения автоматически подтянуться sdk, если не передавать параметры
# OTEL_EXPORTER_OLTP_ENDPOINT=http://localhost:4318
# OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="http://localhost:4318/v1/metrics"
# OTEL_EXPORTER_OTLP_TIMEOUT=10000 (10s)
# OTEL_EXPORTER_OTLP_METRICS_TIMEOUT=10000 (10s)

reader = PeriodicExportingMetricReader(OTLPMetricExporter())
meterProvider = MeterProvider(resource=otel_resource, metric_readers=[reader])
metrics.set_meter_provider(meterProvider)

meter = metrics.get_meter(name=f"{OTEL_SERVICE_NAME}_meter")

payments_processed_total = meter.create_counter(
    name="payments_processed_total",
    description="Total number of processed payments"
)

payments_succeeded_total = meter.create_counter(
    name="payments_succeeded_total",
    description="Total number of successfully processed payments"
)

payments_failed_total = meter.create_counter(
    name="payments_failed_total",
    description="Total number of payments failed to process"
)
