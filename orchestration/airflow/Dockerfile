FROM apache/airflow:2.10.4

ARG SURICATA_UPDATE_VERSION=1.3.3

USER root
RUN printf "Types: deb\nURIs: http://deb.debian.org/debian\nSuites: bookworm-backports\nComponents: main\nSigned-By: /usr/share/keyrings/debian-archive-keyring.gpg\n" \
        > /etc/apt/sources.list.d/bookworm-backports.sources \
    && apt-get update \
    && apt-get install -y -t bookworm-backports --no-install-recommends bzip2 ca-certificates suricata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir "suricata-update==${SURICATA_UPDATE_VERSION}"

USER root
ENV PATH="/home/airflow/.local/bin:${PATH}"
RUN mkdir -p /etc/suricata/rules /var/lib/suricata/rules /tmp/suricata-test \
    && suricata-update update \
        --suricata-conf /etc/suricata/suricata.yaml \
        --suricata /usr/bin/suricata \
        -o /var/lib/suricata/rules \
        --no-test \
        --fail \
    && test -s /var/lib/suricata/rules/suricata.rules \
    && suricata -T -c /etc/suricata/suricata.yaml -l /tmp/suricata-test \
    && rm -rf /tmp/suricata-test

USER airflow
COPY orchestration/airflow/requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
