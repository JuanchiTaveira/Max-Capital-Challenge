# Tasks

## 1. Contrato de resultado y persistencia aditiva

- [x] 1.1 Agregar `InputPosition`, `ProcessingResult` y el puerto de resultados en aplicación, independientes de tipos Kafka/JDBC, con `APPLIED`/`DUPLICATE`, identidad, ledger y snapshot; verificar compilación e integración con los adaptadores en las tareas siguientes, sin tests de records triviales.
- [x] 1.2 Agregar V2 con `app.processing_results`, PK por posición, outcome y snapshot obligatorios, restricciones de valores y referencias coherentes a identidad/ledger; verificar con PostgreSQL Testcontainers tanto una base vacía como una migración desde V1 con datos, rechazando posiciones duplicadas y referencias inconsistentes sin alterar órdenes/ledger previos.
- [x] 1.3 Implementar búsqueda/inserción JDBC de resultados y recuperación del ER desde ledger, y ampliar `OrderRepository` para devolver el ledger coincidente y el ID al insertar; verificar round-trip completo, distinción de topic/partición con igual offset y comparación JSONB con campos extra, propiedades reordenadas y números equivalentes.
- [x] 1.4 Documentar en DECISIONS el esquema y los supuestos como propuesta hasta contar con integración del caso de uso; ejecutar `.\mvnw.cmd spotless:apply`, los tests SQL correspondientes y `.\mvnw.cmd spotless:check`, y crear un commit Conventional Commit de este incremento sólo si pasan.

## 2. Aplicación atómica y recuperación inmutable

- [x] 2.1 Modificar `ProcessExecutionReport` para recibir posición y devolver el resultado después del commit de `orderTransactionManager`; buscar antes y después del lock, conservar la comparación de identidad previa a transición y guardar resultado/efectos en una transacción. Verificar `APPLIED` nuevo, `DUPLICATE` en otra posición y replay de ambos con orden ya avanzada, comparando resultado completo y cardinalidad de resultados/ledger/contador.
- [x] 2.2 Verificar con transacciones concurrentes en PostgreSQL la misma posición/ER (un único resultado idéntico para ambos) y dos posiciones con igual ER (un `APPLIED`, un `DUPLICATE`, un ledger y un incremento); comprobar que otra key o contenido para una posición existente falla sin sobrescribirla.
- [x] 2.3 Inyectar fallas SQL de test en inserción del resultado y del ledger; demostrar rollback de resultado, orden y contador tanto en creación como en actualización, y recuperación sin doble aplicación al retirar la falla. Verificar también que conflicto de identidad y transición inválida no crean resultados exitosos.
- [x] 2.4 Probar recuperación de resultados desde un segundo contexto de aplicación y después de cerrar/reabrir contextos contra la misma base, sin reconstruir snapshots desde `orders`; incluir ER aplicado con V1 y sin resultado que al reentregarse genera `DUPLICATE`, sin backfill de offsets históricos.
- [x] 2.5 Actualizar DECISIONS con evidencia SQL y límites todavía pendientes, aplicar/comprobar Spotless y ejecutar las pruebas de integración afectadas; crear un commit Conventional Commit del caso de uso sólo con los resultados observados.

## 3. Integración Kafka y frontera de confirmación

- [x] 3.1 Pasar topic/partición/offset desde `ExecutionReportListener` al caso de uso, recibir su resultado y mantener confirmación síncrona posterior al commit SQL, consumo en serie y `read_committed`; adaptar las pruebas Kafka para comprobar resultados `APPLIED` y `DUPLICATE` en posiciones reales, incluidos terminales repetidos y ausencia de resultados de una transacción productora abortada.
- [x] 3.2 Agregar una prueba Kafka/PostgreSQL que falle una confirmación antes de enviarla al broker y después del commit SQL mediante infraestructura exclusiva de test; observar resultado durable, offset pendiente y sucesor sin aplicar, reanudar sin la falla y verificar el mismo resultado recuperado, una sola aplicación y avance posterior del grupo. Registrar que esto no es un kill de proceso ni un rebalanceo durante escritura.
- [x] 3.3 Extender las pruebas de JSON/key inválido, conflicto de identidad y falla SQL para comprobar diagnóstico, ausencia de resultado exitoso, offset sin avance y sucesor pendiente; verificar que retirar la falla SQL permite continuar, mientras un mensaje inválido sigue fallando con la política de F-02.
- [x] 3.4 Documentar en README el contrato de resultados, la migración sin backfill, el diagnóstico y los comandos verificados, manteniendo F-03 pendiente hasta la aceptación integral; actualizar DECISIONS con la frontera comprobada y los límites de F-04/F-05/F-06. Ejecutar tests Kafka/SQL y Spotless, y crear un commit Conventional Commit del incremento.

## 4. Verificación integral y evidencia de F-03

- [ ] 4.1 Ejecutar `.\mvnw.cmd verify`, `.\mvnw.cmd spotless:check` y la regresión de dos instancias `docker compose --profile tools run --build --rm verify-f02`; verificar que permanecen correctos estado, secuencia, deduplicación, consulta y persistencia, sin eliminar volúmenes ni usar un build de imagen como reemplazo de tests.
- [ ] 4.2 Registrar en `verification.md` comandos, resultados observados y correspondencia entre escenarios de ambas delta specs y pruebas; ejecutar `openspec validate --all --strict`. Sólo si se cumplen todos los criterios, marcar F-03 implementada en la tabla del README, ajustar decisiones propuestas a garantías observadas y crear el commit Conventional Commit de cierre. Si una dependencia impide verificar, conservar tareas y feature pendientes y registrar el motivo.

## Workflow follow-up

- Sincronizar las delta specs y archivar este change únicamente después de implementar y verificar todos sus criterios, según AGENTS.md.
- Verificar el archivo y detenerse antes de comenzar F-04; no publicar ni hacer push.
