FROM us.gcr.io/broad-dsp-gcr-public/base/python:debian

RUN apt-get update -y && \
    apt-get install -y python-pip python-dev

# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install -r requirements.txt

RUN pip install gunicorn

COPY . /app

EXPOSE 8080

CMD gunicorn -b :8080 main:app