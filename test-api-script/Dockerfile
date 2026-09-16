FROM python:3.14-alpine
WORKDIR /script-dir

RUN adduser -H -D -s /bin/sh script-user

RUN  pip install --no-cache-dir requests

COPY --chown=script-user:script-user . .

USER script-user

CMD ["python3", "script.py"]