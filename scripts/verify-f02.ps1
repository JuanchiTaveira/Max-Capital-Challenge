#requires -Version 5.1
param([switch]$SkipBuild)

# Lanzador opcional. El runner y sus dependencias se ejecutan dentro de Docker.
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  Write-Error 'No se encuentra Docker. Instalar Docker con Compose y activar el motor Linux.'
  exit 1
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
  $arguments = @('compose', '--profile', 'tools', 'run', '--rm')
  if (-not $SkipBuild) { $arguments += '--build' }
  $arguments += 'verify-f02'
  # Dejar stderr nativo sin redirigir evita falsos errores en Windows PowerShell 5.1.
  $ErrorActionPreference = 'Continue'
  & docker @arguments
  $resultCode = $LASTEXITCODE
} finally {
  Pop-Location
}
exit $resultCode
