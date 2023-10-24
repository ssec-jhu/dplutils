FROM python:3.10

RUN apt-get update && \
    apt-get install -y wget  && \
    pip install --upgrade pip

WORKDIR /opt

RUN wget -q https://github.com/prometheus/prometheus/releases/download/v2.43.0/prometheus-2.43.0.linux-amd64.tar.gz && \
    wget -q https://dl.grafana.com/enterprise/release/grafana-enterprise-9.4.7.linux-amd64.tar.gz && \
    tar -xf prometheus-2.43.0.linux-amd64.tar.gz && \
    tar -xf grafana-enterprise-9.4.7.linux-amd64.tar.gz && \
    rm prometheus-2.43.0.linux-amd64.tar.gz && \
    rm grafana-enterprise-9.4.7.linux-amd64.tar.gz

COPY requirements/ ./py-requirements/
RUN pip install -r py-requirements/prd.txt

COPY . ./dplutils/
RUN pip install ./dplutils

COPY docker/startray.sh .

CMD /bin/bash
