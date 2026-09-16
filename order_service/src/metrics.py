from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
import os

OTEL_SERVICE_NAME: str = os.getenv("OTEL_SERVICE_NAME", "order_service")
# следующие переменные окружения автоматически подтянуться sdk, если не передавать параметры
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
