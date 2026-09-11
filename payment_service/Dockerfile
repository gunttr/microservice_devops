FROM python:3.14-alpine
WORKDIR /app

RUN apk add --no-cache curl && adduser -H -D -s /bin/sh app-user

COPY --chown=app-user:app-user requirements.txt .

RUN  pip install --no-cache-dir -r requirements.txt

COPY --chown=app-user:app-user . .

USER app-user

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]