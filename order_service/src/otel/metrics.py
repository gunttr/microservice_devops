
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from src.otel.resource import OTEL_SERVICE_NAME, otel_resource

reader = PeriodicExportingMetricReader(OTLPMetricExporter())
meterProvider = MeterProvider(resource=otel_resource, metric_readers=[reader])
metrics.set_meter_provider(meterProvider)

meter = metrics.get_meter(name=f"{OTEL_SERVICE_NAME}_meter")

orders_created_total = meter.create_counter(
    name="orders_created_total",
    description="Total number of created orders by the order service"
)

payment_success_orders_total = meter.create_counter(
    name="payment_success_orders_total",
    description="Total number of orders with payment_success status"
)

payment_failed_orders_total = meter.create_counter(
    name="payment_failed_orders_total",
    description="Total number of orders with payment_failed status"
)
