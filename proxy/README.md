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

### 3. Obtain Initial SSL Certificate

Run this one-time command to get the first certificate. This must be done **before** starting the full stack.
NB! replace `<email>` with your email address to receive notifications about certificate expiration.

```bash
docker run -it --rm \
  -p 80:80 \
  -v "$(pwd)/letsencrypt_certs:/etc/letsencrypt" \
  certbot/certbot certonly \
  --standalone \
  --email <email> \
  --agree-tos \
  --no-eff-email \
  -d goggregator-test.unicity.network
```
*Note: We use `$(pwd)/letsencrypt_certs` to create a Docker volume with a predictable name.*

### 4. Generate DH-Params

Run this one-time command to generate the Diffie-Hellman parameter file for added security.

```bash
docker run -it --rm \
  --entrypoint /bin/sh \
  -v "$(pwd)/letsencrypt_certs:/etc/letsencrypt" \
  certbot/certbot \
  -c "openssl dhparam -out /etc/letsencrypt/ssl-dhparams.pem 2048"
```

### 5. Launch the Full Stack

You are now ready to start all services. The `-d` flag runs them in the background.

```bash
docker compose up -d
```

## Verification

1.  Check that all containers are running:
    ```bash
    docker ps
    ```
    You should see `proxy-haproxy` and `proxy-certbot` with a status of `Up`.

2.  Open your web browser and navigate to **`https://goggregator-test.unicity.network`**. The site should load securely.
