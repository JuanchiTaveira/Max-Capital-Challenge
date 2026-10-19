# Verificación de F-03

Fecha: 2026-10-19. Entorno: Windows, JDK 25.0.2, Maven Wrapper 3.9.11,
Docker Desktop con motor Linux 29.8.2. Las imágenes PostgreSQL/Kafka conservan
los digests fijados en Compose. No se borraron volúmenes ni se resetearon offsets.

## Comandos y resultados observados

| Comando | Resultado |
| --- | --- |
| `.\mvnw.cmd spotless:apply` | Fuentes editadas formateadas con Spotless. |
| `.\mvnw.cmd verify '-Dit.test=ProcessingResultsSchemaIT'` | 5 unitarias y 2 integraciones de esquema exitosas. |
| `.\mvnw.cmd verify '-Dit.test=ProcessingResultsIT,ProcessingResultsSchemaIT,OrderPersistenceIT'` | 5 unitarias y 13 integraciones SQL/HTTP exitosas. |
| `.\mvnw.cmd verify '-Dit.test=ExecutionReportKafkaIT'` | 5 unitarias y 5 integraciones Kafka exitosas. |
| `.\mvnw.cmd verify` | BUILD SUCCESS: 5 unitarias y 21 integraciones, 0 fallas, 0 errores, 0 omitidas. |
| `.\mvnw.cmd spotless:check` | BUILD SUCCESS. También ejecutado en validate de cada verify. |
| `docker compose stop app-1 app-2` y `docker compose up -d --build --wait app-1 app-2` | Actualización V1→V2 del entorno existente, ambas instancias saludables. |
| `docker compose --profile tools run --build --rm verify-f02` | F-02 OK con dos instancias, FILLED/CANCELLED, duplicados, consulta y persistencia tras reiniciar. |
| Bloques manuales PowerShell del README | Ejecutados con Windows PowerShell 5.1; F-03 MANUAL OK, ver detalle debajo. |
| `openspec validate --all --strict` | Sin errores antes del cierre; specs sincronizadas y validación repetida al archivar. |

La suite completa ejecutó `ExecutionReportKafkaIT` (5), `OrderPersistenceIT` (5),
`ProcessingResultsIT` (6), `ProcessingResultsSchemaIT` (2) y
`RuntimeInfrastructureIT` (3). `OrderDomainTest` conserva 5 pruebas unitarias.
Los tests corren con Kafka/PostgreSQL Testcontainers reales; los dos contextos de
`ProcessingResultsIT` contienen el caso de uso y adaptadores con proxy transaccional
JDBC, sin levantar servidores HTTP adicionales.

## Correspondencia de escenarios y pruebas

| Escenarios de las delta specs | Evidencia ejecutada |
| --- | --- |
| Newly applied report; Business duplicate at another position | `ProcessingResultsIT.recoversOriginalAppliedAndDuplicateSnapshotsAfterTheOrderAdvances` y prueba intercalada Kafka. |
| Position identity includes topic and partition | `ProcessingResultsSchemaIT.roundTripsResultsAndComparesFullJsonWithoutDependingOnFormatting`: igual offset con otro topic/partición, round-trip y restricciones. |
| Replay of applied report after order advances; Replay of duplicate result | `ProcessingResultsIT.recoversOriginalAppliedAndDuplicateSnapshotsAfterTheOrderAdvances` y `recoversFromAnotherApplicationAndAfterClosingAllContexts`: mismo outcome, ledger y snapshot completo. |
| Changed input at an existing position | `ProcessingResultsIT.rejectsChangedPositionAndBusinessIdentityWithoutRecordingSuccess`: payload modificado, otra identidad/key y validación de key canónica. |
| Result write fails after order writes; Ledger write fails | `ProcessingResultsIT.rollsBackAllWritesWhenResultOrLedgerInsertionFailsAndRetriesCleanly`: triggers de test en ambas tablas, creación y actualización, rollback y retry. |
| Concurrent same-position delivery; Concurrent republications | `ProcessingResultsIT.serializesConcurrentSamePositionAndConcurrentRepublications`: dos contextos, dos transacciones, arranque coordinado y una sola aplicación. |
| Invalid report or conflicting business identity; Invalid transition; Invalid input remains uncommitted | `ExecutionReportKafkaIT.keepsInvalidInputsPendingWithNoSuccessResultOrSuccessorApplication`: topics de test separados para JSON, key, identidad y transición; diagnóstico capturado, offset pendiente, resultado ausente y sucesor sin aplicar, también al reiniciar el consumidor. |
| Successful retry after rollback; Database failure | Prueba de rollback SQL y `ExecutionReportKafkaIT.keepsSqlFailureUncommittedAndResumesTheSameSequenceOnRestart`: resultado fallido ausente, sin avance de offset y recuperación en secuencia al retirar la falla. |
| SQL committed and offset confirmation fails | `ExecutionReportKafkaIT.recoversCommittedResultWhenOffsetCommitFailsBeforeReachingKafka`: interceptor de Consumer exclusivo de test falla commitSync antes del broker, después de confirmar SQL; conserva APPLIED y el sucesor pendiente; al reanudar conserva la fila original y continúa. |
| Republished terminal duplicate | `ExecutionReportKafkaIT.processesInterleavedOrdersAndReplayAndIgnoresAbortedReports`: resultados por offsets reales, terminal DUPLICATE sin nuevo ledger, offset confirmado después del resultado. |
| Committed input only, regresión | La prueba intercalada Kafka también verifica ausencia de resultado y ledger para ER de transacción productora abortada. |
| Migración sin backfill y ER previo sin posición | `ProcessingResultsSchemaIT.upgradesV1WithDataWithoutInventingHistoricalResults` y `ProcessingResultsIT.recordsDuplicateForLegacyLedgerWithoutBackfillingUnknownPositions`. |

## Actualización del entorno existente

Antes del despliegue había V1, 8 órdenes y 28 entradas de ledger. Se detuvieron
ambas aplicaciones, se reconstruyeron y se inició F-03 con V2. Antes de emitir
nuevos ER se verificó que los snapshots de las tablas existentes fueran iguales:

| Tabla | MD5 de filas ordenadas antes y después |
| --- | --- |
| orders | `4b5dfeeed531977e46d20ac7c58aee69` |
| execution_report_ledger | `90cf8fd39c89d86f7e6bf78255ac41a3` |

Flyway registró V1 y V2 exitosas. `processing_results` tenía cero filas: no se
inventaron posiciones históricas.

El smoke posterior creó `179243559377301` (partición 0, FILLED, contador/ledger 4)
y `179243559377320` (partición 1, CANCELLED, contador/ledger 3). Ambas instancias
consultaron el estado esperado antes/después del reinicio. La consulta SQL
posterior mostró 4 APPLIED + 2 DUPLICATE para la primera y 3 APPLIED + 2 DUPLICATE
para la segunda: 11 resultados y 7 entradas nuevas de ledger.

## Prueba manual reproducida

Se ejecutaron los tres bloques operativos de la sección «Prueba manual de F-03 en
PowerShell» del README en una misma sesión Windows PowerShell 5.1. El ejecutor
local de verificación sólo agregó esperas por consumo asíncrono y aserciones
HTTP/SQL; no cambió los mensajes, productor, consultas ni reinicio documentados.

Orden conservada para inspección: `1792435633442`. Después de NEW, FILLED,
republicación de FILLED y republicación de NEW, ambas instancias devolvieron
FILLED, contador 2 y ledger de dos entradas. Tras reiniciar ambas y republicar
FILLED se observó:

| Partición/offset | Outcome | fixId | ledgerId | Snapshot | Contador |
| --- | --- | --- | --- | --- | --- |
| 1/25 | APPLIED | 1 | 36 | NEW | 1 |
| 1/26 | APPLIED | 2 | 37 | FILLED | 2 |
| 1/27 | DUPLICATE | 2 | 37 | FILLED | 2 |
| 1/28 | DUPLICATE | 1 | 36 | FILLED | 2 |
| 1/29 | DUPLICATE | 2 | 37 | FILLED | 2 |

Salida final: `F-03 MANUAL OK order=1792435633442 results=5 ledger=2 counter=2`.
Las consultas siguen disponibles en `http://localhost:8081/orders/1792435633442`
y `http://localhost:8082/orders/1792435633442`. El entorno quedó levantado y sus
datos de prueba se conservaron.

## Límites de la evidencia

- La prueba manual republica JSON en offsets nuevos; no fuerza reentrega de la
  misma posición ni cambia offsets del entorno.
- La prueba de frontera inyecta una excepción de confirmación antes del broker.
  No verifica kill real, rebalanceo durante escritura ni respuesta incierta de
  commit SQL/Kafka: F-05 sigue pendiente.
- No hay settlements ni transacción de salida Kafka. Esta evidencia no demuestra
  publicación exactamente una vez para downstream `read_committed`: F-04 sigue
  pendiente.
- Los errores permanecen visibles y pendientes; no hay cuarentena, clasificación
  persistida de fallas ni recuperación automática: F-06 sigue pendiente.
- No se prueba pérdida física de volúmenes, recreación de topics, cambios de
  cluster ni resets arbitrarios del grupo.
