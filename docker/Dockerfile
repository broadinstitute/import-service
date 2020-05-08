FROM python:3.7.4

RUN apt-get update -y && \
    apt-get install -y python-pip python-dev

# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt

RUN wget -O google-cloud-sdk.tar.gz https://storage.googleapis.com/cloud-sdk-release/google-cloud-sdk-276.0.0-linux-x86.tar.gz \
    && tar -xzf google-cloud-sdk.tar.gz \

RUN mkdir /.ssh
ENV PATH /google-cloud-sdk/bin:$PATH
VOLUME ["/.config"]


WORKDIR /app

RUN pip install -r requirements.txt

RUN pip install gunicorn

COPY . /app

EXPOSE 8080

CMD gunicorn -b :8080 main:app