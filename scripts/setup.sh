#!/bin/bash

# Script to setup correct user permissions for Docker containers
# This ensures that files created by containers are owned by the current user

set -e

# Get current user's UID and GID
CURRENT_UID=$(id -u)
CURRENT_GID=$(id -g)

echo "Setting up Docker environment for user $(whoami)"
echo "UID: $CURRENT_UID"
echo "GID: $CURRENT_GID"

# Update .env file with current user's UID and GID
cat > .env << EOF
# Docker user permissions
# These are set to your current user to avoid permission issues
UID=$CURRENT_UID
GID=$CURRENT_GID
EOF

echo "Updated .env file with your user permissions"

# Create data directories with proper permissions
echo "Creating data directories..."
mkdir -p data/genesis data/genesis-root data/mongodb_data

# Set umask to ensure files are created with proper permissions
umask 022

echo "Setup completed!"
echo ""
echo "You can now run:"
echo "  docker-compose up --build"
echo ""
echo "To clean up everything (including data), run:"
echo "  ./scripts/cleanup.sh"
