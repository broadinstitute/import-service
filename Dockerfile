FROM python:3.7.4

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
