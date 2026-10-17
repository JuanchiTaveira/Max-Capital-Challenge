#requires -Version 7.0
[CmdletBinding()]
param(
  [ValidateRange(30, 600)]
  [int]$TimeoutSeconds = 180,
  [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

function Invoke-Compose {
  param([Parameter(Mandatory)][string[]]$Arguments)
  $output = & docker compose @Arguments 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "docker compose $($Arguments -join ' ') falló:`n$($output -join "`n")"
  }
  return ($output | Out-String).Trim()
}

function Get-ProbeStatus {
  param([int]$Port, [string]$Probe)
  try {
    $response = Invoke-WebRequest -Uri "http://localhost:$Port/actuator/health/$Probe" `
      -TimeoutSec 12 -SkipHttpErrorCheck
    return [int]$response.StatusCode
  } catch {
    return 0
  }
}

function Wait-Probes {
  param([int]$ExpectedStatus)
  $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
  do {
    $first = Get-ProbeStatus -Port 8081 -Probe readiness
    $second = Get-ProbeStatus -Port 8082 -Probe readiness
    if ($first -eq $ExpectedStatus -and $second -eq $ExpectedStatus) { return }
    Start-Sleep -Seconds 1
  } while ([DateTime]::UtcNow -lt $deadline)
  throw "Readiness esperado $ExpectedStatus; observado app-1=$first, app-2=$second"
}

function Assert-Liveness {
  foreach ($port in @(8081, 8082)) {
    if ((Get-ProbeStatus -Port $port -Probe liveness) -ne 200) {
      throw "Liveness dejó de estar saludable en $port"
    }
  }
}

function Invoke-Sql {
  param([Parameter(Mandatory)][string]$Sql)
  # La consulta es un argumento separado; las credenciales se leen dentro del contenedor.
  return Invoke-Compose -Arguments @(
    'exec', '-T', 'postgres', 'sh', '-c',
    'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -Atc "$1"',
    'sh', $Sql
  )
}

function Assert-KafkaMarker {
  param([string]$Topic, [string]$Marker)
  $output = Invoke-Compose -Arguments @(
    'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-consumer.sh',
    '--bootstrap-server', 'localhost:29092', '--topic', $Topic,
    '--from-beginning', '--max-messages', '1', '--timeout-ms', '20000',
    '--consumer-property', 'isolation.level=read_committed',
    '--consumer-property', 'enable.auto.commit=false'
  )
  if (($output -split '\r?\n') -notcontains $Marker) {
    throw 'El marcador Kafka no se conservó'
  }
}

$root = Split-Path -Parent $PSScriptRoot
$suffix = [Guid]::NewGuid().ToString('N')
$schema = "f01_smoke_$suffix"
$topic = "f01-smoke-$suffix"
$marker = "persistent-$suffix"
$schemaCreated = $false
$topicCreated = $false
$started = $false
$failure = $null

Push-Location $root
try {
  Write-Host '[1/6] Validar y levantar Compose'
  [void](Invoke-Compose -Arguments @('config', '--quiet'))
  $startup = @('up', '-d')
  if (-not $SkipBuild) { $startup += '--build' }
  [void](Invoke-Compose -Arguments $startup)
  $started = $true
  Wait-Probes -ExpectedStatus 200
  Assert-Liveness
  foreach ($entry in @(@(8081, 'app-1'), @(8082, 'app-2'))) {
    $info = Invoke-RestMethod -Uri "http://localhost:$($entry[0])/actuator/info" -TimeoutSec 10
    if ($info.app.'instance-id' -ne $entry[1] -or
        $info.app.version -ne '0.1.0-SNAPSHOT' -or
        $info.app.name -ne 'max-capital-challenge') {
      throw "Identidad inesperada en $($entry[0])"
    }
  }

  Write-Host '[2/6] Verificar init idempotente y topics'
  [void](Invoke-Compose -Arguments @('run', '--rm', 'kafka-init'))
  foreach ($name in @('execution-reports', 'settlements', 'execution-reports-rejected')) {
    $description = Invoke-Compose -Arguments @(
      'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh',
      '--bootstrap-server', 'localhost:29092', '--describe', '--topic', $name
    )
    if ($description -notmatch 'PartitionCount:\s*2' -or
        $description -notmatch 'retention.ms=-1' -or
        $description -notmatch 'retention.bytes=-1') {
      throw "Configuración inesperada del topic $name"
    }
  }

  Write-Host '[3/6] Crear datos aislados para verificar persistencia'
  [void](Invoke-Sql -Sql "CREATE SCHEMA $schema")
  $schemaCreated = $true
  [void](Invoke-Sql -Sql "CREATE TABLE $schema.marker (value text NOT NULL); INSERT INTO $schema.marker VALUES ('$marker')")
  [void](Invoke-Compose -Arguments @(
    'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh',
    '--bootstrap-server', 'localhost:29092', '--create', '--topic', $topic,
    '--partitions', '2', '--replication-factor', '1'
  ))
  $topicCreated = $true
  $output = $marker | & docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh `
    --bootstrap-server localhost:29092 --topic $topic --producer-property acks=all 2>&1
  if ($LASTEXITCODE -ne 0) { throw "Publicación de marcador falló: $output" }
  Assert-KafkaMarker -Topic $topic -Marker $marker

  Write-Host '[4/6] Caída y recuperación de PostgreSQL'
  [void](Invoke-Compose -Arguments @('stop', 'postgres'))
  Wait-Probes -ExpectedStatus 503
  Assert-Liveness
  [void](Invoke-Compose -Arguments @('start', 'postgres'))
  Wait-Probes -ExpectedStatus 200

  Write-Host '[5/6] Caída y recuperación de Kafka'
  [void](Invoke-Compose -Arguments @('stop', 'kafka'))
  Wait-Probes -ExpectedStatus 503
  Assert-Liveness
  [void](Invoke-Compose -Arguments @('start', 'kafka'))
  Wait-Probes -ExpectedStatus 200

  Write-Host '[6/6] Recrear contenedores conservando volúmenes'
  [void](Invoke-Compose -Arguments @('down'))
  [void](Invoke-Compose -Arguments @('up', '-d'))
  Wait-Probes -ExpectedStatus 200
  Assert-Liveness
  if ((Invoke-Sql -Sql "SELECT value FROM $schema.marker") -ne $marker) {
    throw 'El marcador SQL no se conservó'
  }
  Assert-KafkaMarker -Topic $topic -Marker $marker
} catch {
  $failure = $_
} finally {
  if ($started) {
    try {
      # Restaurar dependencias si el smoke falló mientras alguna estaba detenida.
      [void](Invoke-Compose -Arguments @('up', '-d'))
      Wait-Probes -ExpectedStatus 200
      if ($schemaCreated) { [void](Invoke-Sql -Sql "DROP SCHEMA $schema CASCADE") }
      if ($topicCreated) {
        [void](Invoke-Compose -Arguments @(
          'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh',
          '--bootstrap-server', 'localhost:29092', '--delete', '--topic', $topic
        ))
      }
    } catch {
      Write-Warning "No se pudo completar restauración/limpieza: $_"
      if ($null -eq $failure) { $failure = $_ }
    }
  }
  Pop-Location
}

if ($null -ne $failure) {
  Write-Error -Message "F-01 smoke FALLÓ: $failure" -ErrorAction Continue
  exit 1
}
Write-Host 'F-01 smoke OK: dos instancias, salud, init idempotente y persistencia verificadas.'
exit 0
