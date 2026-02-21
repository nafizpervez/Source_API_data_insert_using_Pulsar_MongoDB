param(
  [switch]$RemoveVolumes
)

$ErrorActionPreference = "Stop"

# Make paths relative to the script location (not current directory)
$Root = Split-Path -Parent $PSScriptRoot          # ...\infra\docker
$EnvFile = Join-Path $Root ".env"
$ComposePath = Join-Path $Root "docker-compose.yml"

Write-Host "Stopping containers..." -ForegroundColor Cyan
docker compose --env-file $EnvFile -f $ComposePath down

if ($RemoveVolumes) {
  Write-Host "Removing volumes..." -ForegroundColor Yellow
  docker compose --env-file $EnvFile -f $ComposePath down -v
}

Write-Host "Done." -ForegroundColor Green