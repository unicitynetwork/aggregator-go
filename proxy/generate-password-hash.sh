#!/bin/bash
# Generate HAProxy password hash for Basic Auth

if [ -z "$1" ]; then
    echo "Usage: $0 <password>"
    echo ""
    echo "This script generates a SHA-512 password hash for HAProxy Basic Auth"
    echo ""
    echo "Example:"
    echo "  $0 'mySecurePassword123'"
    exit 1
fi

PASSWORD="$1"

# Generate SHA-512 hash using mkpasswd (requires whois package on Ubuntu/Debian)
if command -v mkpasswd >/dev/null 2>&1; then
    HASH=$(mkpasswd -m sha-512 "$PASSWORD")
    echo "Generated hash:"
    echo "$HASH"
    echo ""
    echo "Add this to haproxy.cfg userlist:"
    echo "user admin password $HASH"
else
    # Fallback: use openssl
    echo "mkpasswd not found, using openssl (less secure)..."
    HASH=$(openssl passwd -6 "$PASSWORD")
    echo "Generated hash:"
    echo "$HASH"
    echo ""
    echo "Add this to haproxy.cfg userlist:"
    echo "user admin password $HASH"
fi

echo ""
echo "To use in haproxy.cfg:"
echo "userlist dozzle_users"
echo "    user admin password $HASH"
