param(
  [string]$ComposeFile = "..\docker-compose.yml",
  [switch]$RemoveVolumes
)

$ErrorActionPreference = "Stop"

Write-Host "Stopping containers..." -ForegroundColor Cyan
docker compose --env-file "..\.env" -f $ComposeFile down

if ($RemoveVolumes) {
  Write-Host "Removing volumes..." -ForegroundColor Yellow
  docker compose --env-file "..\.env" -f $ComposeFile down -v
}

Write-Host "Done." -ForegroundColor Green