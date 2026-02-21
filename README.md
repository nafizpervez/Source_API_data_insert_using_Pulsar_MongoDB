# api-integration — Source API → Pulsar → MongoDB (Docker Compose)

## 1) Remove Existing Containers + Images (Clean Reset)

From the root folder:

`\Source_API_data_insert_using_Pulsar_MongoDB`

### A) Stop + remove containers (and optionally volumes)

```powershell
docker compose -p api-integration-f .\infra\docker\docker-compose.yml --env-file .\infra\docker\.env down
```

**Why this command**

- Stops and removes containers for this stack (`api-integration`)
- Keeps MongoDB data (volumes) by default

**Optional: delete MongoDB data too (full reset)**

```powershell
docker compose -p api-integration -f .\infra\docker\docker-compose.yml --env-file .\infra\docker\.env down -v
```

**Force: Remove Containers + volumes + orphans**

```powershell
docker compose -p api-integration -f .\infra\docker\docker-compose.yml --env-file .\infra\docker\.env down --remove-orphans -v
```

**If Doesn't work then use**

```powershell
docker rm -f (docker ps -aq)
```

**Why this command**

- Removes volumes as well, so MongoDB starts fresh on next run

### B) Remove the worker image (forces rebuild from scratch)

```powershell
docker rmi -f dotnet-worker
```

### Remove the whole Container with Cache

```powershell
docker compose -p api-integration -f .\infra\docker\docker-compose.yml --env-file .\infra\docker\.env down -v --rmi all
```

**Why this command**

- Ensures the worker image is rebuilt cleanly (no stale layers)

> If your worker image name differs, list images first:

```powershell
docker images
```

---

## 2) Create / Build the Containers (Build Step)

Run from root:

```powershell
docker compose -p api-integration -f .\infra\docker\docker-compose.yml --env-file .\infra\docker\.env build  --no-cache
```

**Why this command**

- Builds the `worker` service image from your Dockerfile
- Does not start containers yet (build-only)

---

## 3) Run / Start the Containers

Run from root:

```powershell
docker compose -p api-integration -f .\infra\docker\docker-compose.yml --env-file .\infra\docker\.env up -d
```

**Why this command**

- Starts all services in detached mode:
  - Pulsar
  - MongoDB
  - SourceIngestor worker
  - dozzle

> If you prefer a single command to build + run in one:

```powershell
docker compose -p api-integration -f .\infra\docker\docker-compose.yml --env-file .\infra\docker\.env up -d --build
```

---

## 4) Change MongoDB Username/Password (Single Source of Truth)

### A) Edit credentials in `.env` (recommended)

File:
`infra\docker\.env`

Change:

```env
MONGO_INITDB_ROOT_USERNAME=admin
MONGO_INITDB_ROOT_PASSWORD=admin123
```

**Why this approach**

- You maintain Mongo credentials in ONE place only (`.env`)
- Compose and the worker consume those values automatically

### B) IMPORTANT NOTE (Mongo init behavior)

Mongo only applies `MONGO_INITDB_ROOT_USERNAME / PASSWORD` the **first time** the volume is created.

If Mongo was already initialized and you change the credentials later:

- run a full reset (deletes Mongo data):

```powershell
docker compose -p api-integration -f .\infra\docker\docker-compose.yml --env-file .\infra\docker\.env down -v
```

---

## 5) MongoDB Compass Connection String

If `.env` contains:

- `MONGO_PORT=27017`
- `MONGO_INITDB_ROOT_USERNAME=admin`
- `MONGO_INITDB_ROOT_PASSWORD=admin123`

Use this in MongoDB Compass:

```text
mongodb://admin:admin123@localhost:27017/?authSource=admin
```

---

## 6) Codes in Sequence for fast running (only use these to run the full project)

```powershell
docker compose -p api-integration -f .\infra\docker\docker-compose.yml --env-file .\infra\docker\.env down -v --rmi all
```

```powershell
docker compose -p api-integration -f .\infra\docker\docker-compose.yml --env-file .\infra\docker\.env build --no-cache
```

```powershell
docker compose -p api-integration -f .\infra\docker\docker-compose.yml --env-file .\infra\docker\.env up -d
```

---

## 7) Logs (Operational Verification)

### Worker logs

```powershell
docker logs -f DotNet-Worker
```

### Pulsar logs

```powershell
docker logs -f Pulsar-Message-Broker
```

### Mongo logs

```powershell
docker logs -f Mongo
```

---

## 8) Expected Worker Logs (Success Path)

You should see log lines like:

- `SourceCheckProcessor starting...`
- `Seeded SourceCheckJob into persistent://public/default/source-check`
- `API availability check: True`
- `Fetched JSON length: ...`
- `Published posts batch to persistent://public/default/posts-batch`
- `MongoWriter received batch message. size=...`
- `Inserted batch into MongoDB. itemCount=100`

This confirms:

- API fetch succeeded
- Pulsar publish succeeded
- Mongo insert succeeded (single batch document containing 100 items)
