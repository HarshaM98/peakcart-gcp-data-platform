#!/bin/bash
# Dataproc initialization action: installs and starts the Cloud SQL Auth
# Proxy on the master node, so the Spark job can reach Cloud SQL PostgreSQL
# via IAM auth on localhost rather than a whitelisted public IP.
set -euo pipefail

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role || echo "Master")

if [[ "$ROLE" == "Master" ]]; then
  curl -sL -o /usr/local/bin/cloud-sql-proxy \
    https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.14.0/cloud-sql-proxy.linux.amd64
  chmod +x /usr/local/bin/cloud-sql-proxy

  INSTANCE_CONNECTION_NAME=$(/usr/share/google/get_metadata_value attributes/instance-connection-name)

  nohup /usr/local/bin/cloud-sql-proxy "${INSTANCE_CONNECTION_NAME}" --port 5432 \
    > /var/log/cloud-sql-proxy.log 2>&1 &
fi
