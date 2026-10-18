param([switch]$SkipBuild)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot

function Invoke-Compose {
  param([string[]]$Arguments)
  $result = & docker compose @Arguments 2>&1
  if ($LASTEXITCODE -ne 0) { throw "docker compose $Arguments falló: $($result -join "`n")" }
  return $result
}

function Wait-Check {
  param([scriptblock]$Check, [string]$Description, [int]$TimeoutSeconds = 150)
  $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
  $lastFailure = ''
  do {
    try { & $Check; return } catch { $lastFailure = $_.Exception.Message }
    Start-Sleep -Seconds 1
  } while ([DateTimeOffset]::UtcNow -lt $deadline)
  throw "Timeout: $Description. $lastFailure"
}

function Assert-That {
  param([bool]$Condition, [string]$Message)
  if (-not $Condition) { throw $Message }
}

# Particionado estándar Kafka (murmur2) para elegir una orden en cada partición.
function Get-Partition {
  param([long]$OrderId)
  $bytes = [Text.Encoding]::UTF8.GetBytes($OrderId.ToString([Globalization.CultureInfo]::InvariantCulture))
  [ulong]$mask = 4294967295
  [ulong]$m = 1540483477
  [ulong]$h = 2538058380 -bxor $bytes.Length
  $i = 0
  while ($i + 4 -le $bytes.Length) {
    [ulong]$k = [ulong]$bytes[$i] -bor ([ulong]$bytes[$i + 1] -shl 8) -bor
      ([ulong]$bytes[$i + 2] -shl 16) -bor ([ulong]$bytes[$i + 3] -shl 24)
    $k = ($k * $m) -band $mask
    $k = $k -bxor ($k -shr 24)
    $k = ($k * $m) -band $mask
    $h = (($h * $m) -band $mask) -bxor $k
    $i += 4
  }
  $remaining = $bytes.Length - $i
  if ($remaining -ge 3) { $h = $h -bxor ([ulong]$bytes[$i + 2] -shl 16) }
  if ($remaining -ge 2) { $h = $h -bxor ([ulong]$bytes[$i + 1] -shl 8) }
  if ($remaining -ge 1) {
    $h = $h -bxor [ulong]$bytes[$i]
    $h = ($h * $m) -band $mask
  }
  $h = $h -bxor ($h -shr 13)
  $h = ($h * $m) -band $mask
  $h = $h -bxor ($h -shr 15)
  return [int](($h -band 2147483647) % 2)
}

function Get-OrderId {
  param([long]$Start, [int]$Partition)
  while ((Get-Partition $Start) -ne $Partition) { $Start++ }
  return $Start
}

function New-Report {
  param([long]$OrderId, [long]$FixId, [string]$Status, [int]$Accumulated, [int]$Leaves)
  return [ordered]@{
    fixId = $FixId; numericOrderId = $OrderId; status = $Status
    nominalAmounts = 10; accumulativeNominalAmount = $Accumulated; leavesNominalAmount = $Leaves
    transactionTime = '2026-07-20T18:08:52.129'; ticker = 'VSCPC'
    executionPrice = 104.30; secondaryTradeId = "smoke-$OrderId-$FixId"
  }
}

function Publish-Reports {
  param([object[]]$Reports)
  $lines = foreach ($report in $Reports) { "$($report.numericOrderId)|$($report | ConvertTo-Json -Compress)" }
  $output = ($lines -join "`n") | & docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh `
    --bootstrap-server localhost:29092 --topic execution-reports --property parse.key=true `
    --property 'key.separator=|' --producer-property acks=all 2>&1
  if ($LASTEXITCODE -ne 0) { throw "Publicación falló: $output" }
}

function Wait-Applications {
  Wait-Check -Description 'readiness de ambas instancias' -Check {
    foreach ($port in @(8081, 8082)) {
      $health = Invoke-RestMethod "http://localhost:$port/actuator/health/readiness" -TimeoutSec 5
      Assert-That ($health.status -eq 'UP') "Readiness de $port no está UP"
    }
  }
  Wait-Check -Description 'dos consumidores con una partición cada uno' -Check {
    $members = (Invoke-Compose -Arguments @('exec', '-T', 'kafka',
      '/opt/kafka/bin/kafka-consumer-groups.sh', '--bootstrap-server', 'localhost:29092',
      '--describe', '--group', 'max-capital-orders-v1', '--members', '--verbose')) -join "`n"
    Assert-That ($members -match 'execution-reports[:(]0\)?' -and $members -match 'execution-reports[:(]1\)?') $members
    Assert-That (([regex]::Matches($members, 'execution-reports[:(][01]\)?')).Count -eq 2) $members
  }
}

function Wait-Consumed {
  Wait-Check -Description 'offsets confirmados sin lag en ambas particiones' -Check {
    $offsets = (Invoke-Compose -Arguments @('exec', '-T', 'kafka',
      '/opt/kafka/bin/kafka-consumer-groups.sh', '--bootstrap-server', 'localhost:29092',
      '--describe', '--group', 'max-capital-orders-v1')) -join "`n"
    $caughtUp = [regex]::Matches($offsets, '(?m)^\S+\s+execution-reports\s+[01]\s+\d+\s+\d+\s+0\s+\S+')
    Assert-That ($caughtUp.Count -eq 2) $offsets
  }
}

function Assert-Order {
  param([long]$OrderId, [string]$Status, [int]$Count, [int]$Accumulated)
  $views = foreach ($port in @(8081, 8082)) {
    $order = Invoke-RestMethod "http://localhost:$port/orders/$OrderId" -TimeoutSec 5
    Assert-That ($order.status -eq $Status) "Estado incorrecto en $port"
    Assert-That ($order.appliedExecutionCount -eq $Count -and $order.ledger.Count -eq $Count) 'Contador o ledger incorrecto'
    Assert-That ($order.accumulativeNominalAmount -eq $Accumulated -and $order.leavesNominalAmount -eq 0) 'Snapshot incorrecto'
    [long]$previous = 0
    for ($index = 0; $index -lt $Count; $index++) {
      $entry = $order.ledger[$index]
      Assert-That ($entry.ledgerId -gt $previous) 'Ledger fuera de orden'
      Assert-That ($entry.report.fixId -eq $index + 1) 'Secuencia fixId incorrecta'
      Assert-That ($entry.report.ticker -eq 'VSCPC') 'Se perdió un campo adicional'
      $previous = $entry.ledgerId
    }
    $order | ConvertTo-Json -Depth 15 -Compress
  }
  Assert-That ($views[0] -eq $views[1]) 'Las instancias devuelven estados diferentes'
}

try {
  $arguments = @('up', '-d', '--wait')
  if (-not $SkipBuild) { $arguments += '--build' }
  Invoke-Compose -Arguments $arguments | Out-Host
  Wait-Applications
  [long]$baseId = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 100
  $a = Get-OrderId -Start $baseId -Partition 0
  $b = Get-OrderId -Start ($baseId + 20) -Partition 1
  $filled = New-Report $a 4 'FILLED' 10 0
  $cancelled = New-Report $b 3 'CANCELLED' 7 0
  $reports = @(
    (New-Report $a 1 'NEW' 0 10), (New-Report $b 1 'NEW' 0 10),
    (New-Report $a 2 'PARTIALLY_FILLED' 3 7), (New-Report $b 2 'PARTIALLY_FILLED' 7 3),
    (New-Report $a 3 'PARTIALLY_FILLED' 7 3), $cancelled, $filled, $filled, $cancelled
  )
  Publish-Reports -Reports $reports
  Wait-Consumed
  Wait-Check -Description 'estado y ledger iniciales' -Check {
    Assert-Order $a 'FILLED' 4 10
    Assert-Order $b 'CANCELLED' 3 7
  }
  Write-Host "Secuencia, snapshots y duplicados verificados: orden $a (partición 0), $b (partición 1)."
  Invoke-Compose -Arguments @('restart', 'app-1', 'app-2') | Out-Host
  Wait-Applications
  Publish-Reports -Reports @($filled, $cancelled)
  Wait-Consumed
  Wait-Check -Description 'persistencia tras reinicio y reentrega terminal' -Check {
    Assert-Order $a 'FILLED' 4 10
    Assert-Order $b 'CANCELLED' 3 7
  }
  Write-Host 'F-02 OK: ambas instancias consumen y consultan; estado y ledger persisten tras reinicio.'
  Write-Host 'Se conservan los volúmenes y las órdenes de prueba con IDs aislados para inspección.'
} finally {
  Pop-Location
}
