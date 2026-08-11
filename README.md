# Ephemeral Relay

Ephemeral Relay is a nostr relay that accepts only a configured set of event kinds and deletes everything past a fixed age. Nothing here is forever: the relay's NIP-11 description states its retention policy up front, and a periodic sweep hard-deletes anything older than the window. Events can also opt into dying sooner via [NIP-40](https://github.com/nostr-protocol/nips/blob/master/40.md) expiration tags, which are fully honoured.

It's built on the [Khatru](https://khatru.nostr.technology) framework.

## Use Cases

This relay suits anything whose natural lifetime is hours, not years:

- **Live stream chat**: chat messages, reactions, reposts and zap receipts live for the duration of a show and then scroll away, like chat is supposed to.
- **Bridged content**: when you republish messages on behalf of users from another platform who never signed up for permanent, globally-replicated speech, a relay that forgets is a feature.
- **Ephemeral notice boards**: announcements, presence, status — anything where stale content is worse than no content.

## How It Works

- **Kind allowlist**: only the kinds in `ALLOWED_KINDS` are accepted; everything else is rejected at publish. The default set (`0,5,7,16,1311,1312,1313,9735`) is live-chat flavoured: profiles, deletion requests, reactions, generic reposts, live chat messages, and zap receipts — and deliberately **not** kind 1 notes.
- **Age-based retention**: a periodic sweep hard-deletes events older than `RETENTION_SECONDS`. Kinds listed in `RETENTION_EXEMPT_KINDS` (profiles, by default) are kept indefinitely, so identities outlive the chat.
- **NIP-40 expiration**, three layers:
  - events that arrive already expired are rejected;
  - expired events are never served, from the exact second they lapse, independent of sweep timing;
  - the sweep hard-deletes expired events from the store, whatever their kind or age.

  The expiration tag can only shorten a life — a distant expiration does not exempt an event from the blanket retention window.
- **Rate limiting**: per-IP token bucket on writes, with a `TRUSTED_IPS` exemption for your own infrastructure (for example a bridge that funnels many streams' chat through a single egress IP).

## Prerequisites

- **Go**: Ensure you have Go installed on your system. You can download it from [here](https://golang.org/dl/).
- **Build Essentials**: If you're using Linux, you may need to install build essentials. You can do this by running `sudo apt install build-essential`.

## Setup Instructions

Follow these steps to get Ephemeral Relay running on your local machine:

### 1. Clone the repository

```bash
git clone https://github.com/r0d8lsh0p/ephemeral-relay.git
cd ephemeral-relay
```

### 2. Copy `.env.example` to `.env`

You'll need to create an `.env` file based on the example provided in the repository.

```bash
cp .env.example .env
```

### 3. Set your environment variables

Open the `.env` file and set the necessary environment variables:

```bash
RELAY_NAME="chat.yourdomain.com"
RELAY_PUBKEY="YourPublicKey" # the owner's hexkey, not npub
RELAY_ICON="https://yourdomain.com/icon.png"
# RELAY_DESCRIPTION is auto-generated from the retention settings unless you set it.

PORT=3335
DB_PATH="db/" # any path you would like the database to be saved

# Only these kinds are accepted; everything else is rejected at publish.
ALLOWED_KINDS="0,5,7,16,1311,1312,1313,9735"

RETENTION_SECONDS=10800 # events older than this are hard-deleted (3 hours)
PURGE_INTERVAL_SECONDS=600 # how often the deletion sweep runs
RETENTION_EXEMPT_KINDS="0" # kinds kept indefinitely (profiles by default)

RATE_LIMIT_EVENTS_PER_SEC=10 # per-IP write rate limit
RATE_LIMIT_BURST=50
TRUSTED_IPS="" # comma-separated IPs exempt from the rate limit, e.g. your bridge
```

### 4. Build the project

Run the following command to build the relay:

```bash
go build
```

### 5. Create a Systemd Service (optional)

To have the relay run as a service, create a systemd unit file.

1. Create the file:

```bash
sudo nano /etc/systemd/system/ephemeral-relay.service
```

2. Add the following contents:

```ini
[Unit]
Description=Ephemeral Relay Service
After=network.target

[Service]
ExecStart=/home/ubuntu/ephemeral-relay/ephemeral-relay
WorkingDirectory=/home/ubuntu/ephemeral-relay
Restart=always

[Install]
WantedBy=multi-user.target
```

Replace `/home/ubuntu/` with the actual path where you cloned the repository.

3. Reload systemd to recognize the new service:

```bash
sudo systemctl daemon-reload
```

4. Start the service:

```bash
sudo systemctl start ephemeral-relay
```

5. (Optional) Enable the service to start on boot:

```bash
sudo systemctl enable ephemeral-relay
```

#### Permission Issues on Some Systems

The relay may not have permissions to read and write to the database. To fix this, you can change the permissions of the database folder:

```bash
sudo chmod -R 777 /path/to/db
```

### 6. Serving over nginx (optional)

You can serve the relay over nginx by adding the following configuration to your nginx configuration file:

```nginx
server {
    listen 80;
    server_name chat.yourdomain.com;

    location / {
        proxy_pass http://localhost:3335;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

Replace `chat.yourdomain.com` with your actual domain name. Note that `X-Forwarded-For` matters here: the rate limiter identifies clients by real IP through the proxy, and `TRUSTED_IPS` entries are matched against those.

After adding the configuration, restart nginx:

```bash
sudo systemctl restart nginx
```

### 7. Install Certbot (optional)

If you want to serve the relay over HTTPS, you can use Certbot to generate an SSL certificate.

```bash
sudo apt-get update
sudo apt-get install certbot python3-certbot-nginx
```

After installing Certbot, run the following command to generate an SSL certificate:

```bash
sudo certbot --nginx
```

Follow the instructions to generate the certificate.

### 8. Access the relay

Once everything is set up, the relay will be running on `localhost:3335` or your domain name if you set up nginx. Fetch its NIP-11 document to confirm the policy it's advertising:

```bash
curl -H 'Accept: application/nostr+json' https://chat.yourdomain.com
```

## Start the Project with Docker Compose

To start the project using Docker Compose, follow these steps:

1. Ensure Docker and Docker Compose are installed on your system.
2. Navigate to the project directory.
3. Adjust the environment variables in `docker-compose.yml` as needed.
4. Run the following command:

   ```sh
   # in foreground
   docker compose up --build
   # in background
   docker compose up --build -d
   ```

5. For updating the relay, run the following command:

   ```sh
   git pull
   docker compose build --no-cache
   docker compose up -d
   ```

This will build the Docker image and start the `relay` service as defined in the `docker-compose.yml` file. The application will be accessible on port 7448 (mapped from the container's 3335).

## End-to-End Tests

A protocol-level checker lives in `e2e/`. Point it at a relay running with short retention:

```bash
RETENTION_SECONDS=8 PURGE_INTERVAL_SECONDS=2 PORT=3336 DB_PATH=/tmp/eph-e2e ./ephemeral-relay &
go run ./e2e -relay ws://localhost:3336 -retention 8
```

Checks: disallowed kinds rejected; chat accepted and served; after the retention window chat is purged while a kind 0 profile survives.

Additional modes:

```bash
go run ./e2e -relay ws://localhost:3336 -nip40 -ttl 180 # NIP-40: short-TTL event dies on time, tagless control survives
go run ./e2e -relay ws://localhost:3336 -burst-only # rate limiter caps an untrusted burst
go run ./e2e -relay ws://localhost:3336 -burst-only -burst-trusted # TRUSTED_IPS bypass takes the full volley
```

## License

MIT
