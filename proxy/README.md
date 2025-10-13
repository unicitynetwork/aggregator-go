# HAProxy and Let's Encrypt Deployment

This guide outlines the steps to deploy the HAProxy reverse proxy with automatic SSL certificate generation via Let's Encrypt.

## Prerequisites

1.  A fresh Linux server (e.g., Ubuntu 22.04).
2.  Your domain name's DNS A record pointing to the new server's public IP address.
3.  Docker and Docker Compose installed.

    ```bash
    # Install Docker
    sudo apt-get update
    sudo apt-get install -y docker.io

    # Install Docker Compose
    sudo apt-get install -y docker-compose

    # Add your user to the docker group to run commands without sudo
    sudo usermod -aG docker ${USER}
    
    # IMPORTANT: Log out and log back in for the group change to take effect.
    ```

## Deployment Steps

The order of these steps is critical.

### 1. Copy Configuration Files

Copy your project directory (which contains `docker-compose.yml` and the `haproxy/` subdirectory) to the new server.

Example using `scp`:
```bash
scp -r /path/to/your/project/ user@your_new_server_ip:~/
```
Navigate into the project directory on the new server.

### 2. Configure DNS

Ensure your domain **`goggregator-test.unicity.network`** points to the new server's IP address. Let's Encrypt will fail if this is not correct. Wait a few minutes for DNS to propagate.

### 3. Start Temporary Nginx for Initial Certificate

First, create a volume and start a temporary nginx server to handle the ACME challenge:

```bash
# Create the webroot volume
docker volume create proxy_acme_webroot

# Start temporary nginx on port 80 to serve ACME challenges
docker run -d --name temp-nginx \
  -p 80:80 \
  -v proxy_acme_webroot:/usr/share/nginx/html \
  nginx:alpine

# Configure nginx to serve the webroot
docker exec temp-nginx sh -c "echo 'server {
  listen 80;
  server_name _;
  location /.well-known/acme-challenge/ {
    root /usr/share/nginx/html;
  }
}' > /etc/nginx/conf.d/default.conf"

# Reload nginx
docker exec temp-nginx nginx -s reload
```

### 4. Obtain Initial SSL Certificate

Run this one-time command to get the first certificate using webroot mode.
NB! replace `<email>` with your email address to receive notifications about certificate expiration.

```bash
docker run -it --rm \
  -v "$(pwd)/letsencrypt_certs:/etc/letsencrypt" \
  -v proxy_acme_webroot:/var/www/certbot \
  certbot/certbot certonly \
  --webroot \
  --webroot-path=/var/www/certbot \
  --email <email> \
  --agree-tos \
  --no-eff-email \
  -d goggregator-test.unicity.network
```

### 5. Generate DH-Params and HAProxy Certificate

Run these commands to generate security parameters and prepare the certificate for HAProxy:

```bash
# Generate DH parameters
docker run -it --rm \
  --entrypoint /bin/sh \
  -v "$(pwd)/letsencrypt_certs:/etc/letsencrypt" \
  certbot/certbot \
  -c "openssl dhparam -out /etc/letsencrypt/ssl-dhparams.pem 2048"

# Create HAProxy-compatible certificate file
docker run -it --rm \
  -v "$(pwd)/letsencrypt_certs:/etc/letsencrypt" \
  certbot/certbot \
  sh -c "cat /etc/letsencrypt/live/goggregator-test.unicity.network/privkey.pem /etc/letsencrypt/live/goggregator-test.unicity.network/fullchain.pem > /etc/letsencrypt/live/goggregator-test.unicity.network/haproxy.pem && chmod -R 755 /etc/letsencrypt/live && chmod -R 755 /etc/letsencrypt/archive"
```

### 6. Stop Temporary Nginx and Launch the Full Stack

Now stop the temporary nginx and start all services:

```bash
# Stop and remove temporary nginx
docker stop temp-nginx && docker rm temp-nginx

# Start all services
docker compose up -d
```

## Verification

1.  Check that all containers are running:
    ```bash
    docker ps
    ```
    You should see three containers with status `Up`:
    - `proxy-haproxy` - HAProxy reverse proxy
    - `proxy-certbot` - Certificate renewal service
    - `proxy-nginx-acme` - ACME challenge server

2.  Open your web browser and navigate to **`https://goggregator-test.unicity.network`**. The site should load securely with a valid SSL certificate.

3.  Check certificate renewal logs:
    ```bash
    docker logs proxy-certbot
    ```
    You should see successful renewal checks every 12 hours.

## Automatic Renewal

The setup includes automatic certificate renewal with the following features:

- **Renewal Schedule**: Certbot checks for renewal twice per day (every 12 hours)
- **Renewal Method**: Uses webroot authentication via the nginx-acme container
- **Zero Downtime**: HAProxy stays online during renewals; certificates are reloaded via graceful reload signal
- **No Manual Intervention**: The entire renewal process is automated
- **Certificate Updates**: When renewed, certificates are automatically converted to HAProxy format and permissions are fixed

### How It Works

1. Certbot checks if certificates need renewal (Let's Encrypt certificates are valid for 90 days; renewal happens at 30 days before expiry)
2. If renewal is needed, certbot places challenge files in the shared webroot volume
3. Let's Encrypt validation server requests the challenge via HTTP (port 80)
4. HAProxy proxies `/.well-known/acme-challenge/` requests to nginx-acme on port 8080
5. Nginx serves the challenge files from the webroot
6. After successful validation, certbot receives the new certificate
7. The deploy hook combines the certificate files for HAProxy and triggers a graceful reload
8. HAProxy loads the new certificate without dropping connections

### Troubleshooting Renewal

If renewal fails, check:

```bash
# Check certbot logs
docker logs proxy-certbot

# Check nginx-acme is serving files
curl http://localhost:8080/.well-known/acme-challenge/test

# Manually test renewal (dry run)
docker exec proxy-certbot certbot renew --dry-run --webroot --webroot-path=/var/www/certbot
```
