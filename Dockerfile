FROM python:3.9.5 AS build

RUN python3 -m venv /venv

# Install Python dependencies into the virtualenv
COPY requirements.txt /
RUN /venv/bin/pip install -r /requirements.txt
RUN /venv/bin/pip install gunicorn

FROM us.gcr.io/broad-dsp-gcr-public/base/python:debian

RUN apt-get update -q && \
    apt-get install -qq --no-install-recommends \
      build-essential \
      libffi-dev && \
    pip3 install -U pip && \
    pip3 install -r requirements.txt && \
    apt-get remove -y \
      build-essential \
      libffi-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /venv /venv
COPY . .

EXPOSE 8080

CMD /venv/bin/gunicorn -b :8080 main:app
