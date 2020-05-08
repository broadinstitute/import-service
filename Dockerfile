FROM python:3.7.4

RUN apt-get update -y && \
    apt-get install -y -qq --no-install-recommends wget tar build-essential openssh-client python-openssl python-pip python-dev && \
    apt-get clean

COPY requirements.txt /app/requirements.txt

ENV HOME /
ENV CLOUDSDK_PYTHON_SITEPACKAGES 1
RUN wget -O google-cloud-sdk.tar.gz https://storage.googleapis.com/cloud-sdk-release/google-cloud-sdk-291.0.0-linux-x86.tar.gz \
    && tar -xzf google-cloud-sdk.tar.gz \
    && google-cloud-sdk/install.sh \
        --usage-reporting=true --path-update=true --bash-completion=true --rc-path=/.bashrc --additional-components \
        app-engine-python app

RUN mkdir /.ssh
ENV PATH /google-cloud-sdk/bin:$PATH
VOLUME ["/.config"]

WORKDIR app/

RUN pip install -r requirements.txt

RUN pip install gunicorn

COPY . /app

EXPOSE 8080

CMD gunicorn -b :8080 main:app