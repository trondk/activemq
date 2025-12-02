podman pull icr.io/ibm-messaging/mq:9.4.4.0-r3

podman run -d --name ibmmq --env LICENSE=accept --env MQ_QMGR_NAME=QM1 -p 1414:1414 -p 9443:9443 icr.io/ibm-messaging/mq:9.4.4.0-r3

podman logs -f ibmmq
