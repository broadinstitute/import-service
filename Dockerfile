FROM python:3.9

# Configure Poetry
ENV POETRY_VERSION=1.2.0
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VENV=/opt/poetry-venv
ENV POETRY_CACHE_DIR=/opt/.cache

# Install poetry separated from system interpreter
RUN python3 -m venv $POETRY_VENV \
    && $POETRY_VENV/bin/pip install -U pip setuptools \
    && $POETRY_VENV/bin/pip install poetry==${POETRY_VERSION}

# Add `poetry` to PATH
ENV PATH="${PATH}:${POETRY_VENV}/bin"

WORKDIR /app

# Install dependencies
COPY pyproject.toml ./
COPY poetry.lock ./

RUN poetry lock
RUN poetry install
RUN poetry add gunicorn

FROM us.gcr.io/broad-dsp-gcr-public/base/python:3.9-debian

WORKDIR /app
COPY --from=build /venv /venv
COPY . .

EXPOSE 8080

CMD /venv/bin/gunicorn -b :8080 main:app
