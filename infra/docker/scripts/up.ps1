param(
  [string]$ComposeFile = "..\docker-compose.yml"
)

$ErrorActionPreference = "Stop"

# Make paths relative to the script location (not current directory)
$Root = Split-Path -Parent $PSScriptRoot          # ...\infra\docker
$EnvFile = Join-Path $Root ".env"
$ComposePath = Join-Path $Root "docker-compose.yml"

Write-Host "Starting containers..." -ForegroundColor Cyan
docker compose --env-file $EnvFile -f $ComposePath up -d

Write-Host ""
Write-Host "Containers status:" -ForegroundColor Cyan
docker ps

Write-Host ""
$envData = Get-Content $EnvFile | Where-Object { $_ -match "^(PULSAR_HTTP_PORT|MONGO_EXPRESS_PORT)=" }
$mongoExpressPort = ($envData | Where-Object { $_ -match "^MONGO_EXPRESS_PORT=" }) -replace "^MONGO_EXPRESS_PORT=", ""
$pulsarHttpPort   = ($envData | Where-Object { $_ -match "^PULSAR_HTTP_PORT=" }) -replace "^PULSAR_HTTP_PORT=", ""

Write-Host "Mongo Express (optional UI): http://localhost:$mongoExpressPort" -ForegroundColor Green
Write-Host "Pulsar Admin:               http://localhost:$pulsarHttpPort"   -ForegroundColor Green