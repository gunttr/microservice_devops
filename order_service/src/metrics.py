from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

resource = Resource.create(attributes={
    SERVICE_NAME: "order_service"
})

reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="http://otel-collector:4318/v1/metrics", timeout=10)
)
meterProvider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meterProvider)

meter = metrics.get_meter(name="order_service_meter")

orders_created_counter = meter.create_counter(
    name="orders_created_total",
    description="Total number of created orders"
)

payment_success_orders_counter = meter.create_counter(
    name="payment_success_orders_total",
    description="Total number of orders with payment status payment_success"
)

payment_failed_orders_counter = meter.create_counter(
    name="payment_failed_orders_total",
    description="Total number of orders with payment status payment_failed"
)
