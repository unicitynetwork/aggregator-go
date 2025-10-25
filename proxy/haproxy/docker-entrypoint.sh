#!/bin/sh
# HAProxy entrypoint script that substitutes environment variables

set -e

CONFIG_TEMPLATE="/usr/local/etc/haproxy/haproxy.cfg"
CONFIG_FINAL="/tmp/haproxy.cfg"

echo "Preparing HAProxy configuration..."

# Check if password hash is set
if [ -z "$DOZZLE_PASSWORD_HASH" ]; then
    echo "WARNING: DOZZLE_PASSWORD_HASH not set!"
    echo "The /logs endpoint will reject all authentication attempts."
    echo "Set it with: ./proxy/generate-password-hash.sh 'yourpassword'"
fi

# Substitute ${DOZZLE_PASSWORD_HASH} with the actual value using sed
sed "s|\${DOZZLE_PASSWORD_HASH}|${DOZZLE_PASSWORD_HASH}|g" "$CONFIG_TEMPLATE" > "$CONFIG_FINAL"

echo "Configuration prepared. Starting HAProxy..."

# Start HAProxy with the processed config
exec haproxy -W -db -f "$CONFIG_FINAL" "$@"
