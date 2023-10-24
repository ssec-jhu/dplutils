#!/bin/bash

if [[ $@ =~ "--head" ]]; then
    ray start --disable-usage-stats --include-dashboard true --dashboard-host 0.0.0.0 $@
    /opt/prometheus-2.43.0.linux-amd64/prometheus --config.file=/tmp/ray/session_latest/metrics/prometheus/prometheus.yml &
    /opt/grafana-9.4.7/bin/grafana server --homepath /opt/grafana-9.4.7/ --config /tmp/ray/session_latest/metrics/grafana/grafana.ini web &
else
    ray start $@
fi
