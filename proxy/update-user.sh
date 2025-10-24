#!/bin/bash
# Helper script to add/update users in HAProxy users.cfg

set -e

USERS_FILE="$(dirname "$0")/haproxy/users.cfg"

show_usage() {
    echo "Usage: $0 <username> <password>"
    echo ""
    echo "This script adds or updates a user in HAProxy users.cfg and reloads HAProxy"
    echo ""
    echo "Examples:"
    echo "  $0 admin newPassword123       # Update admin password"
    echo "  $0 developer dev@2025         # Add new user"
    echo ""
}

if [ $# -ne 2 ]; then
    show_usage
    exit 1
fi

USERNAME="$1"
PASSWORD="$2"

# Check if users.cfg exists
if [ ! -f "$USERS_FILE" ]; then
    echo "Error: $USERS_FILE not found!"
    exit 1
fi

# Generate password hash
echo "Generating password hash for user '$USERNAME'..."
HASH=$(openssl passwd -6 "$PASSWORD")

# Check if user already exists
if grep -q "user $USERNAME " "$USERS_FILE"; then
    echo "User '$USERNAME' exists, updating password..."
    # Use sed to update the password in place
    sed -i.bak "s|user $USERNAME password.*|user $USERNAME password $HASH|" "$USERS_FILE"
else
    echo "Adding new user '$USERNAME'..."
    # Add new user before the closing comment
    sed -i.bak "/^# To add more users:/i\\    user $USERNAME password $HASH" "$USERS_FILE"
fi

echo "✓ User '$USERNAME' updated in $USERS_FILE"
echo ""
echo "To apply changes:"
echo "  1. Validate configuration:"
echo "     docker exec proxy-haproxy haproxy -f /usr/local/etc/haproxy/haproxy.cfg -c"
echo ""
echo "  2. Reload HAProxy (graceful, no downtime):"
echo "     docker exec proxy-haproxy kill -SIGUSR2 1"
echo ""
echo "  Or simply restart:"
echo "     docker compose -f proxy/docker-compose.yml restart haproxy"
