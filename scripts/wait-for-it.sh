#!/bin/bash
# wait-for-it.sh

# Wait for a service to be ready
# Usage: ./wait-for-it.sh <host>:<port> -- <command>
# Example: ./wait-for-it.sh db:5432 -- airflow db init

HOST=$1
PORT=$2
shift 2

# Retry interval in seconds
RETRIES=60
WAIT_INTERVAL=5

# Check if the service is up
for i in $(seq 1 $RETRIES); do
    nc -z $HOST $PORT && break
    echo "Waiting for $HOST:$PORT to be available ($i/$RETRIES)..."
    sleep $WAIT_INTERVAL
done

if [ $i -eq $RETRIES ]; then
    echo "$HOST:$PORT did not become available in time!"
    exit 1
fi

echo "$HOST:$PORT is available, running the command..."
exec "$@"
