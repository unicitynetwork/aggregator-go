#!/bin/bash

# Script to fix file permissions for existing Docker installations
# This script resolves permission issues when containers can't access mounted files

set -e

echo "Fixing file permissions for Docker containers..."

# Function to fix permissions on a file or directory
fix_permissions() {
    local path="$1"
    local type="$2"  # "file" or "dir"
    
    if [ -e "$path" ]; then
        echo "Fixing permissions on $path..."
        
        # Change ownership to current user if needed
        if [ "$(stat -c '%U' "$path")" = "root" ]; then
            echo "  - Changing ownership from root to $(whoami)"
            sudo chown -R $(whoami):$(whoami) "$path"
        fi
        
        # Set appropriate permissions
        if [ "$type" = "file" ]; then
            chmod 644 "$path"
        else
            chmod -R 755 "$path"
        fi
        
        echo "  - Permissions fixed"
    else
        echo "Path $path does not exist, skipping..."
    fi
}

# Fix permissions on data directories
echo "Fixing data directory permissions..."
fix_permissions "./data" "dir"

# Fix specific configuration files
echo "Fixing configuration file permissions..."
fix_permissions "./data/genesis/trust-base.json" "file"
fix_permissions "./data/genesis/shard-conf-7_0.json" "file"

# Fix directories
fix_permissions "./data/genesis/aggregator" "dir"
fix_permissions "./data/genesis/root" "dir"
fix_permissions "./data/genesis-root" "dir"
fix_permissions "./data/mongodb_data" "dir"

echo "Permission fixes completed!"
echo ""
echo "You can now restart the containers:"
echo "  docker-compose restart"
echo ""
echo "Or do a full restart:"
echo "  docker-compose down && docker-compose up -d"
