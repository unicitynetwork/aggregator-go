#!/bin/bash

# Script to clean up Docker volumes and data directories
# This script handles permission issues that may occur when containers run as different users

set -e

echo "Stopping and removing containers..."
docker compose down -v --remove-orphans

echo "Cleaning up data directories..."

# Function to safely remove directory with sudo if needed
safe_remove() {
    local dir="$1"
    if [ -d "$dir" ]; then
        echo "Removing $dir..."
        if rm -rf "$dir" 2>/dev/null; then
            echo "Successfully removed $dir"
        else
            echo "Permission denied, trying with sudo..."
            sudo rm -rf "$dir"
            echo "Successfully removed $dir with sudo"
        fi
    else
        echo "Directory $dir does not exist, skipping..."
    fi
}

# Remove data directories
safe_remove "./data/genesis"
safe_remove "./data/genesis-root"
safe_remove "./data/mongodb_data"

# Remove the entire data directory if it's empty
if [ -d "./data" ] && [ -z "$(ls -A ./data)" ]; then
    echo "Removing empty data directory..."
    rmdir ./data
fi

echo "Cleanup completed!"
