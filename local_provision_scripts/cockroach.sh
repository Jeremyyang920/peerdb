#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"

DOCKER="docker"
CONTAINER="peerdb-cockroach"

# Sinkless (core) changefeeds require the rangefeed cluster setting. It is a
# cluster-wide setting best applied once the node is accepting SQL, so we set it
# here rather than in the container command. Idempotent: safe to re-run.
echo "enabling kv.rangefeed.enabled"
$DOCKER exec "$CONTAINER" cockroach sql --insecure -e "SET CLUSTER SETTING kv.rangefeed.enabled = true;"

echo "cockroach is ready at ${CI_COCKROACH_HOST}:${CI_COCKROACH_PORT}"
