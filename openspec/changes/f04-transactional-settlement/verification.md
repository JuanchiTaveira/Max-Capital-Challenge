# Verification

## Estado de la evidencia

Implementación y pruebas dirigidas ejecutadas el 2026-10-21. La verificación global
y el recorrido Compose se completan antes de cambiar el estado F-04 del README.
No se atribuye evidencia a la existencia de una tarea marcada ni a un build de imagen.

## Comandos observados

| Comando | Resultado |
| --- | --- |
| `.\mvnw.cmd verify -Dtest=SettlementTest -Dit.test=ProcessingResultsIT` | SUCCESS: 2 unitarias y 7 integraciones, cero fallas/omisiones; contrato y commit SQL previo al publicador |
| `.\mvnw.cmd verify -Dtest=SettlementTest -Dit.test=ExecutionReportKafkaIT` | SUCCESS: 2 unitarias y 5 integraciones, cero fallas/omisiones; inyección actualizada a commitTransaction |
| `.\mvnw.cmd verify -Dtest=SettlementTest -Dit.test=SettlementKafkaIT` | SUCCESS: 2 unitarias y 9 integraciones, cero fallas/omisiones; incluida JVM hija terminada abruptamente y reasignación real |

La primera ejecución de la suite nueva detectó una suposición incorrecta en su
observador: AdminClient devuelve un mapa parcial omitiendo la partición con
UNSTABLE_OFFSET_COMMIT, en lugar de mantener necesariamente el future abierto.
Se corrigió el test para observar la espera mediante KafkaConsumer.committed con
read_committed. La siguiente ejecución completa de las 9 integraciones pasó.
El protocolo de producción no se alteró para acomodar ese observador.

## Correspondencia de escenarios

Los nombres de casos referencian SettlementKafkaIT salvo indicación expresa.

| Spec / escenario | Evidencia |
| --- | --- |
| settlement-publication / First completion | publishesOnePerFilledWithTwoMembersAndNoCancellationOrLateDuplicateOutput: una salida por orden FILLED, comparada byte a byte con el resultado original |
| Pending original completion is redelivered | keepsAcknowledgedSendInvisibleUntilAbortAndRecoversOriginalResult: mismo resultado SQL antes/después, dos intentos y una salida confirmada |
| Republished terminal and late duplicate | publishesOnePerFilledWithTwoMembersAndNoCancellationOrLateDuplicateOutput: FILLED y NEW republicados, contador/ledger intactos y sólo dos salidas para dos órdenes FILLED |
| Cancellation after partial execution | mismo caso: orden 10003 con parcial 4 y CANCELLED, cero salidas incluso tras republicar |
| Invalid terminal | ExecutionReportKafkaIT.keepsInvalidInputsPendingWithNoSuccessResultOrSuccessorApplication y preservesPoisonRecordAndDoesNotSkipToTheNextRecord; decisión de exclusión de SettlementTest |
| Output matches original completion | publishesOnePerFilledWithTwoMembersAndNoCancellationOrLateDuplicateOutput: key y JSON completos contra proyección del resultado SQL recuperado |
| Stable reconstruction with exact values | SettlementTest.preservesExactIdentifiersAndCanonicalDecimalsAcrossSqlScale y ProcessingResultsIT.publishesAfterSqlCommitAndRecoversTheExactOutputWithoutPublishingOnRollback; cierre/reapertura de contexto y precisión NUMERIC |
| Two active instances and business duplicates | publishesOnePerFilledWithTwoMembersAndNoCancellationOrLateDuplicateOutput: dos miembros reales, una partición por instancia, lector assign no agrega miembros |
| Old owner resumes after reassignment | fencesOldOwnerAfterRealPollTimeoutAndReassignment: generación expira por max.poll.interval=5000, B obtiene partición, se libera A y su commit falla por metadatos de grupo obsoletos |
| Send acknowledged before abort | keepsAcknowledgedSendInvisibleUntilAbortAndRecoversOriginalResult: espera del Future de send antes de la barrera, read_committed vacío y posterior aborto/replay con una salida |
| SQL visible while publication is pending | mismo caso y recoversAfterKillingTheProcessBetweenSqlAndPublication: SQL FILLED consultable mientras la salida permanece invisible |
| Process dies between SQL commit and publication | recoversAfterKillingTheProcessBetweenSqlAndPublication: PID de hijo validado contra barrera, destroyForcibly sin cierre Spring, B recupera, A vuelve y no duplica |
| Broker commits but response is lost | resumesAfterActualCommitWithLostResponseWithoutResending: commit real delegado, salida/offset observados antes de TimeoutException, productor cerrado y un solo send total tras reiniciar |
| Preexisting pending F03 completion | activatesPendingF03CompletionWithoutBackfillingAcknowledgedHistory: fixtures SQL sin publicador y offset pendiente; se conserva resultado y aparece una salida |
| Preexisting acknowledged completion | mismo caso: offset confirmado antes de activar listener; republicación terminal DUPLICATE, cero salidas para esa orden |
| execution-report-consumption / Database failure | rollsBackTerminalSqlBeforeAnyOutputAndResumesWithoutSkipping y pruebas SQL existentes: trigger falla después de escrituras, rollback conjunto, sin salida ni avance |
| Invalid transition | ExecutionReportKafkaIT.keepsInvalidInputsPendingWithNoSuccessResultOrSuccessorApplication: caso transition conserva error y sucesor pendiente |
| SQL committed and offset confirmation fails | ExecutionReportKafkaIT.recoversCommittedResultWhenOffsetCommitFailsBeforeReachingKafka: commitTransaction falla una vez, resultado original se conserva al retomar |
| Republished terminal duplicate | caso normal y activación histórica: posición nueva DUPLICATE confirma progreso sin salida adicional |
| Invalid input remains uncommitted | ExecutionReportKafkaIT: json/key/identity/transition vuelven a fallar al reiniciar, sin skip ni resultado exitoso |
| Output and offset share commit | keepsAcknowledgedSendInvisibleUntilAbortAndRecoversOriginalResult, caso de respuesta perdida y caso pending: evidencia antes y después de resolución Kafka |
| Nonterminal progress is transactional | ExecutionReportKafkaIT.recoversCommittedResultWhenOffsetCommitFailsBeforeReachingKafka: NEW SQL confirmado con progreso pendiente y posterior replay original |
| Uncertain commit stops the current session | resumesAfterActualCommitWithLostResponseWithoutResending y waitsForPendingTransactionalOffsetsThenReplaysAfterResolution: dos desenlaces distintos, nueva sesión y progreso estable |
| Partition ownership is lost during processing | fencesOldOwnerAfterRealPollTimeoutAndReassignment: se espera reasignación real, se libera propietario anterior y se cuenta salida después de terminar ambos intentos |

## Método y límites

Los fixtures crean topics y órdenes aislados en Kafka/PostgreSQL reales con las
imágenes fijadas del proyecto. La lectura de salidas asigna ambas particiones sin
unirse al grupo, recorre hasta sus endOffsets read_committed y cuenta por orden.
Los intentos anteriores se liberan/finalizan antes de afirmar unicidad final.
Controles y barreras sólo existen en src/test; no hay endpoints de falla en producto.

El caso lost-reply delega un commit real y luego arroja un timeout por debajo del
wrapper de productor de Spring. Demuestra pérdida controlada de respuesta, no
corte físico de red. El caso pending no envía commit al broker, conserva offsets
transaccionales pendientes y observa la espera del consumidor hasta su resolución.
El kill sí termina un proceso JVM real; el caso de reasignación sí pierde la
generación del grupo. No se presentan cierres ordenados como caídas abruptas.

La activación F-03 usa fixtures del caso de uso SQL y confirmación previa del grupo,
sin ejecutar publicador durante la preparación; no se ejecuta un binario F-03 viejo.
La evidencia de ledger legado se conserva en ProcessingResultsIT y no autoriza
inventar posiciones o salidas históricas.

La garantía es un registro confirmado en settlements por FILLED cubierto, no un
commit distribuido ni efectos únicos de un consumidor downstream. Depende de
conservar broker/base, grupo, retención y particionado. F-05 conserva ampliaciones
de recuperación, y F-06 el bloqueo de mensajes permanentes. La consola no incorpora
un escenario guiado nuevo; su lector existente permite inspeccionar las salidas.

## Recorrido Compose observado

`docker compose up -d --build --wait` terminó con exit 0 y PostgreSQL, Kafka,
app-1, app-2 y consola saludables. `docker compose --profile tools run --build
--rm verify-f04` terminó con exit 0. Se conservan los datos anteriores y los del
recorrido, sin borrar volúmenes ni resetear el grupo.

- FILLED: orden `179259575427500`, contador/ledger 3, exactamente una salida.
- CANCELLED con parcial: orden `179259575427521`, contador/ledger 3, cero salidas.
- Salida confirmada: `settlements`, partición 0, offset 0, key `179259575427500`.
- Settlement ID `settlement:179259575427500`, fixId 3, ledgerId 194.
- Fuente: `execution-reports`, partición 0, offset 148. Cantidades `10`, `10`, `0`.
- Terminales y NEW tardío republicados no agregaron salida ni efectos SQL.
- El lector de consola recorre ambas particiones sin participar del grupo.
  La comprobación posterior conservó dos miembros con una partición cada uno.

La suite global inicial encontró dos esperas frágiles de DemoPublishingIT: consultaban
una fila todavía ausente mediante queryForObject, cuya excepción no era reintentada
por Awaitility. El helper ahora espera una lista con exactamente el contador esperado;
el resultado exigido sigue siendo el mismo y el plazo continúa en 30 segundos.
La validación global final debe pasar antes de declarar F-04 implementada.
