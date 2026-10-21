# Verification

## Evidencia incremental

La feature sigue en implementación; las tareas abiertas y README conservan ese
límite. No se atribuye evidencia al build de imagen ni a la disponibilidad del catálogo.

- `.\mvnw.cmd spotless:apply test '-Dtest=RecoveryBarrierTest'`: SUCCESS, dos casos,
  cero fallas/errores/omisiones; alcance, tokens, consumo único, vencimiento y cancelación.
- `.\mvnw.cmd spotless:apply verify '-Dtest=RecoveryBarrierTest' '-Dit.test=RecoveryKafkaIT'`:
  SUCCESS, dos unitarias y cuatro integraciones, cero fallas/errores/omisiones.
  Kafka y PostgreSQL reales con imágenes fijadas; los kills usan destroyForcibly
  sobre JVM con PID verificado contra el control de barrera, sin cierre Spring.

## Correspondencia inicial

| Caso de RecoveryKafkaIT | Qué observa |
| --- | --- |
| observesRealSqlBoundariesExpiresWithoutSkippingAndDisablesControlsByDefault | HTTP 404 sin instrumentación; SQL invisible/visible según frontera; envío aceptado invisible; vencimiento deja offset pendiente y replay recupera. |
| killsOpenSqlDuringNewAndUpdateAndRecoversTheEntireSequence | Kill con NEW o actualización SQL abierta; ausencia de orden parcial y de escrituras confirmadas del intento; ledger completo de los sucesores. |
| reassignsWithOpenSqlAndRejectsOldOwnerForCommitAndRollback | Reasignación real por poll antes de liberar SQL; desenlaces commit y rollback del anterior; rechazo de su intento y una sola aplicación. |
| killsAfterAcknowledgedSendAndAfterBothProcessesAndResumesOnBothPartitions | Dos JVM reales, kill después de send y luego de ambas con SQL confirmado; mismo resultado durable, una salida y continuación/CANCELLED sin salida en ambas particiones. |

La suite ampliada, los tests de consola, los smokes y la inspección en navegador
se registrarán al observarlos. La prueba de respuesta perdida de F-04 conserva
su alcance: timeout inyectado después de delegar commit real; no corte físico de red.
