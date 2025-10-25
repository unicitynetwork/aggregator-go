#!/bin/bash
# Generate HAProxy password hash for the admin user

if [ -z "$1" ]; then
    echo "Usage: $0 <password>"
    echo ""
    echo "Generates a SHA-512 password hash and shows how to set it"
    echo ""
    echo "Example:"
    echo "  $0 'mySecurePassword123'"
    exit 1
fi

PASSWORD="$1"

# Generate SHA-512 hash using openssl
HASH=$(openssl passwd -6 "$PASSWORD")

echo "Generated password hash for admin user:"
echo ""
echo "Set this environment variable before starting HAProxy:"
echo ""
echo "export DOZZLE_PASSWORD_HASH='$HASH'"
echo ""
echo "Then deploy/restart:"
echo "docker compose -f proxy/docker-compose.yml up -d"
