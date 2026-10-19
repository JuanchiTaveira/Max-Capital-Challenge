# Proposal

## Why

F-02 evita aplicar dos veces un ER, pero no conserva qué produjo cada registro
Kafka. F-03 debe recuperar ese resultado ante una reentrega sin recalcularlo desde
una orden que pudo avanzar; es la base SQL para F-04 y las verificaciones de F-05.

## What Changes

- Persistir un resultado inmutable por `(topic, partition, offset)` para cada
  procesamiento SQL exitoso: `APPLIED` o `DUPLICATE`, identidad del ER, referencia
  al ledger aplicado y snapshot de la orden al resolver ese registro.
- Confirmar el resultado en la misma transacción JDBC que estado, contador y
  ledger. Una reentrega de la misma posición recupera el resultado original;
  republicar el ER en otra posición registra `DUPLICATE` sin repetir sus efectos.
- Devolver el resultado desde el caso de uso y pasarlo al listener como base para
  la futura coordinación con Kafka. F-03 mantiene la confirmación manual del
  offset después del commit SQL.
- Preservar deduplicación individual, comparación JSONB y consumo secuencial de
  F-02. JSON/key inválidos, conflicto de identidad, transición inválida y errores
  SQL no generan resultados exitosos ni habilitan avance del offset.
- Verificar recuperación del resultado tras cerrar/reabrir la aplicación,
  rollback conjunto, concurrencia y la frontera SQL confirmado/offset pendiente
  mediante una falla controlada de confirmación.

Fuera de alcance: publicar settlements, outbox o transacciones Kafka de salida
(F-04), ensayar caídas reales y rebalanceos (F-05), persistir errores definitivos,
cuarentena o recuperación automática (F-06), nuevos endpoints y cambios al contrato
de identidad del ER. F-03 no demuestra publicación exactamente una vez.

## Capabilities

### New Capabilities

- `processing-results`: resultados SQL por posición Kafka, inmutabilidad,
  recuperación y distinción entre ER aplicado y duplicado de negocio.

### Modified Capabilities

- `execution-report-consumption`: exigir resultado durable antes de confirmar el
  offset y recuperar el mismo resultado cuando SQL confirmó pero Kafka no avanzó.

## Impact

Migración Flyway aditiva sobre PostgreSQL existente; cambios futuros en
`ProcessExecutionReport`, puertos de aplicación, adaptadores JDBC y listener,
con pruebas SQL/Kafka de integración. El dominio y `GET /orders/{numericOrderId}`
conservan su contrato. Sin dependencias, servicios ni topics nuevos. README y
DECISIONS deberán reflejar evidencia y límites al implementar; F-03 permanece
pendiente durante esta propuesta.
