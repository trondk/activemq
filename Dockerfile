FROM golang:1.24.7

RUN mkdir -p /opt/mqm && \
    curl -LO https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/9.4.0.0-IBM-MQC-Redist-LinuxX64.tar.gz && \
    tar -xzf 9.4.0.0-IBM-MQC-Redist-LinuxX64.tar.gz -C /opt/mqm && \
    rm 9.4.0.0-IBM-MQC-Redist-LinuxX64.tar.gz

ENV MQ_INSTALLATION_PATH=/opt/mqm
ENV CGO_CFLAGS="-I/opt/mqm/inc"
ENV CGO_LDFLAGS="-L/opt/mqm/lib64 -Wl,-rpath,/opt/mqm/lib64"

WORKDIR /app
