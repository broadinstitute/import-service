FROM python:3.9.5 AS build

RUN python3 -m venv /venv

# Install Python dependencies into the virtualenv
COPY requirements.txt /
RUN /venv/bin/pip install -r /requirements.txt
RUN /venv/bin/pip install gunicorn

FROM us.gcr.io/broad-dsp-gcr-public/base/python:3.9-debian

WORKDIR /app
COPY --from=build /venv /venv
COPY . .

EXPOSE 8080

CMD /venv/bin/gunicorn -b :8080 main:app
