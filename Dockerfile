FROM python:3.9.1 AS build

RUN python3 -m venv /venv

# Install Python dependencies into the virtualenv
COPY requirements.txt /
RUN /venv/bin/pip install -r /requirements.txt
RUN /venv/bin/pip install gunicorn

FROM us.gcr.io/broad-dsp-gcr-public/base/python:debian

WORKDIR /app
COPY --from=build /venv /venv
COPY . .

EXPOSE 8080

CMD gunicorn -b :8080 main:app
# ENTRYPOINT ["/venv/bin/python3", "main.py"]
