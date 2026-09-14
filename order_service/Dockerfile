FROM python:3.14-slim
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --create-home --shell /bin/sh app-user

COPY --chown=app-user:app-user requirements.txt .

RUN  pip install --no-cache-dir -r requirements.txt

COPY --chown=app-user:app-user . .

USER app-user

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]