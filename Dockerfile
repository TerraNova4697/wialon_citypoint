FROM python:3.10-alpine3.19

ENV PYTHONUNBUFFERED 1

COPY ./requirements /tmp/requirements
COPY . /app
WORKDIR /app
EXPOSE 8101

RUN python -m venv /py && \
    /py/bin/pip install --upgrade pip && \
    apk add --update --no-cache postgresql-client jpeg-dev && \
    apk add --update --no-cache --virtual .tmp-build-deps \
        build-base postgresql-dev musl-dev zlib zlib-dev linux-headers && \
    /py/bin/pip install -r /tmp/requirements/requirements.txt && \
    rm -rf /tmp && \
    apk del .tmp-build-deps && \
    adduser \
        --disabled-password \
        --no-create-home \
        transport-user && \
    mkdir -p /logs/development.log && \
    mkdir -p /logs/test.log && \
    mkdir -p /logs/prod.log && \
    chown -R transport-user:transport-user /logs && \
    chmod 755 /logs

    ENV PATH="/py/bin:$PATH"

    USER transport-user