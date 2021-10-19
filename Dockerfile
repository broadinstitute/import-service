FROM python:3.9.5 AS build

RUN python3 -m venv /venv

COPY requirements.txt /

# Install Python dependencies into the virtualenv
RUN apt-get update -q && \
    apt-get install -qq --no-install-recommends \
      build-essential \
      libffi-dev && \
    /venv/bin/pip3 install -U pip && \
    /venv/bin/pip3 install -r /requirements.txt && \
    /venv/bin/pip3 install gunicorn && \
    apt-get remove -y \
      build-essential \
      libffi-dev && \
    rm -rf /var/lib/apt/lists/*

FROM us.gcr.io/broad-dsp-gcr-public/base/python:debian

WORKDIR /app
COPY --from=build /venv /venv
COPY . .

EXPOSE 8080

CMD /venv/bin/gunicorn -b :8080 main:app
