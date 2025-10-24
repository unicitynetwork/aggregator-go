# HAProxy SSL Reverse Proxy with Auto-Renewing Let's Encrypt Certificates

Fully automated HAProxy reverse proxy with automatic SSL certificate generation and renewal via Let's Encrypt.

## Features

- **Zero-Configuration**: One command deploys everything
- **Automatic SSL**: Obtains and renews Let's Encrypt certificates automatically
- **Zero Downtime**: Certificate renewals happen without service interruption
- **Production Ready**: Secure TLS 1.2+ with modern ciphers

## Prerequisites

1.  A Linux server (e.g., Ubuntu 22.04)
2.  **DNS A record for `goggregator-test.unicity.network` pointing to your server's IP** (CRITICAL!)
3.  Docker and Docker Compose installed
4.  Ports 80 and 443 open in your firewall

### Install Docker (if needed)

```bash
# Install Docker
sudo apt-get update
sudo apt-get install -y docker.io docker-compose

# Add your user to the docker group
sudo usermod -aG docker ${USER}

# IMPORTANT: Log out and log back in for the group change to take effect
```

## Deployment

### 1. Copy Files to Server

```bash
scp -r /path/to/proxy/ user@your_server_ip:~/
ssh user@your_server_ip
cd ~/proxy
```

### 2. Configure Email (Optional)

Set your email for Let's Encrypt notifications:

```bash
export CERTBOT_EMAIL=your-email@example.com
```

Or edit the `docker-compose.yml` file to change the default email.

### 3. Deploy Everything

```bash
docker compose up -d
```

The system will automatically:
1. ✅ Obtain SSL certificate from Let's Encrypt
2. ✅ Generate DH parameters for added security
3. ✅ Configure HAProxy with HTTPS
4. ✅ Start the reverse proxy
5. ✅ Begin automatic renewal checks every 12 hours

**First startup takes 2-3 minutes** (certificate acquisition + DH param generation).

### 4. Setup User Authentication (Optional - for /logs endpoint)

If you want to access Dozzle logs via `/logs`:

```bash
# Copy the example users file
cp proxy/haproxy/users.cfg.example proxy/haproxy/users.cfg

# Add your admin user
./proxy/update-user.sh admin "YourSecurePassword"

# Reload HAProxy (graceful, no restart needed)
docker exec proxy-haproxy kill -SIGUSR2 1
```

**Note:** HAProxy will start successfully with an empty userlist, but `/logs` will return 401 Unauthorized until you add users.

## Verification

### Watch the Automatic Setup

Monitor the initial certificate acquisition:

```bash
docker logs -f proxy-certbot
```

You should see:
- Certificate acquisition from Let's Encrypt
- DH parameter generation
- "Initial setup complete. Service is healthy."

### Check Status

```bash
# Verify all containers are running
docker ps

# Should show:
# - proxy-certbot (healthy)
# - proxy-haproxy (up)
# - proxy-nginx-acme (up)
```

### Test HTTPS

Open your browser: **https://goggregator-test.unicity.network**

You should see:
- ✅ Valid SSL certificate
- ✅ Secure connection (padlock icon)
- ✅ Your application running

## How It Works

### Initial Certificate Acquisition

On first startup:
1. Certbot starts and checks if certificates exist
2. If not, obtains certificates using standalone mode (port 80 is available)
3. Generates DH parameters for security
4. Creates HAProxy-compatible certificate bundle
5. Marks itself as healthy
6. HAProxy and nginx-acme start (they wait for certbot to be healthy)

### Automatic Renewal

After initial setup:
- **Schedule**: Checks every 12 hours
- **Method**: Uses webroot mode through HAProxy (no downtime!)
- **Trigger**: Renews 30 days before expiration
- **Updates**: Automatically updates HAProxy without interruption

**Renewal Flow:**
1. Certbot checks if renewal needed (30 days before expiry)
2. Places challenge files in shared volume
3. Let's Encrypt requests challenge via HTTP
4. HAProxy routes `/.well-known/acme-challenge/` to nginx-acme
5. Validation succeeds, new certificate issued
6. Deploy hook updates HAProxy cert and triggers graceful reload
7. HAProxy loads new cert without dropping connections

## Troubleshooting

### Certificate Acquisition Failed

```bash
# Check certbot logs
docker logs proxy-certbot

# Common issues:
# 1. DNS not pointing to server - verify with: dig goggregator-test.unicity.network
# 2. Firewall blocking port 80 - check: sudo ufw status
# 3. Another service using port 80 - check: sudo netstat -tulpn | grep :80
```

### Renewal Issues

```bash
# Test renewal without actually renewing
docker exec proxy-certbot certbot renew --dry-run --webroot --webroot-path=/var/www/certbot

# Check nginx-acme is working
curl http://localhost:8080/.well-known/acme-challenge/

# View renewal logs
docker logs proxy-certbot | grep -i renew
```

### Force Certificate Renewal

If you need to force a renewal (e.g., after DNS changes):

```bash
# Stop all services
docker compose down

# Remove certificate volume
docker volume rm proxy_letsencrypt_certs

# Restart (will get new certificates)
docker compose up -d
```

## Dozzle Log Viewer Access

The `/logs` endpoint provides access to Dozzle (Docker log viewer) with password protection.

**Access URL:** `https://goggregator-test.unicity.network/logs`

### Initial Setup

**IMPORTANT:** `users.cfg` is gitignored and must be created locally before deploying.

```bash
# 1. Copy the example file
cp proxy/haproxy/users.cfg.example proxy/haproxy/users.cfg

# 2. Add your first user
./proxy/update-user.sh admin "YourSecurePassword"

# 3. Verify users are configured
cat proxy/haproxy/users.cfg

# 4. Then deploy
docker compose -f proxy/docker-compose.yml up -d
```


### Changing Dozzle Password (Easy Method)

Use the helper script:

```bash
./proxy/update-user.sh admin "YourNewPassword"
```

Then reload HAProxy:
```bash
docker exec proxy-haproxy kill -SIGUSR2 1
```

### Adding More Users

```bash
./proxy/update-user.sh developer "dev@2025"
./proxy/update-user.sh viewer "view@2025"
```

Then reload HAProxy:
```bash
docker exec proxy-haproxy kill -SIGUSR2 1
```

### Manual Method

If you prefer to edit manually:

1. Generate password hash:
   ```bash
   ./proxy/generate-password-hash.sh "YourPassword"
   ```

2. Edit `proxy/haproxy/users.cfg` and add/update users:
   ```
   userlist dozzle_users
       user admin password $6$...hash1...
       user viewer password $6$...hash2...
   ```

3. Reload HAProxy (graceful, no downtime):
   ```bash
   docker exec proxy-haproxy kill -SIGUSR2 1
   ```

**Note:** Users are stored in `proxy/haproxy/users.cfg` - you never need to touch `haproxy.cfg`!

## Configuration

### Change Domain

Edit `docker-compose.yml` and update the `DOMAIN` environment variable:

```yaml
environment:
  - DOMAIN=your-domain.com
  - EMAIL=${CERTBOT_EMAIL:-admin@example.com}
```

Also update `haproxy.cfg` to reference the new domain in the certificate path.

### Change Email

Set environment variable before starting:

```bash
export CERTBOT_EMAIL=your-email@example.com
docker compose up -d
```

Or edit `docker-compose.yml` directly.
