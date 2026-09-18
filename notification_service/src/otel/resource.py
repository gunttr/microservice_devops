from opentelemetry.sdk.resources import Resource
import os

OTEL_SERVICE_NAME: str = os.getenv("OTEL_SERVICE_NAME", "notification_service")
otel_resource = Resource.create(attributes={
    "service.name": OTEL_SERVICE_NAME
})