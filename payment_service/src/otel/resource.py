import os

from opentelemetry.sdk.resources import Resource

OTEL_SERVICE_NAME: str = os.getenv("OTEL_SERVICE_NAME", "payment_service")
otel_resource = Resource.create(attributes={
    "service.name": OTEL_SERVICE_NAME
})