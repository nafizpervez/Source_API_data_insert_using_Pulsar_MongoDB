param(
  [string]$ComposeFile = "..\docker-compose.yml"
)

$ErrorActionPreference = "Stop"

Write-Host "Starting containers..." -ForegroundColor Cyan
docker compose --env-file "..\.env" -f $ComposeFile up -d

Write-Host ""
Write-Host "Containers status:" -ForegroundColor Cyan
docker ps

Write-Host ""
Write-Host "Mongo Express (optional UI): http://localhost:$env:MONGO_EXPRESS_PORT" -ForegroundColor Green
Write-Host "Pulsar Admin:               http://localhost:$env:PULSAR_HTTP_PORT" -ForegroundColor Green