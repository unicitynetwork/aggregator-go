#!/bin/bash
# Generate HAProxy password hash and update .env file

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [ -z "$1" ]; then
    echo "Usage: $0 'password'"
    echo ""
    echo "Generates password hash for admin user and updates .env file"
    echo ""
    echo "IMPORTANT: Always use quotes around the password!"
    echo ""
    echo "Examples:"
    echo "  $0 'mySecurePassword123'"
    echo "  $0 'P@ssw0rd!2025'"
    exit 1
fi

PASSWORD="$1"

# Generate SHA-512 hash using openssl
echo "Generating password hash..."
HASH=$(openssl passwd -6 "$PASSWORD")

# For Docker Compose .env files, we need to escape $ as $$
ESCAPED_HASH=$(echo "$HASH" | sed 's/\$/\$\$/g')

# Create or update .env file
echo "DOZZLE_PASSWORD_HASH=$ESCAPED_HASH" > "$ENV_FILE"

echo "✓ Password hash saved to $ENV_FILE"
echo ""
echo "To apply the new password, recreate HAProxy:"
echo "  cd $(dirname $SCRIPT_DIR)"
echo "  docker compose -f proxy/docker-compose.yml up -d haproxy"
echo ""
echo "Then access Dozzle at https://your-domain/logs"
echo "  Username: admin"
echo "  Password: (the password you just set)"
