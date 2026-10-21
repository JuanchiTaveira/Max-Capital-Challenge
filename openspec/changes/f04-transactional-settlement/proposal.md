# Proposal

## Why

Una orden FILLED todavía no produce settlement: SQL y el offset se confirman por
separado. F-03 ya conserva la decisión por posición; F-04 debe aprovecharla para
publicar una sola salida confirmada por orden, recuperable ante reentregas y fallas
con dos instancias, como exige el enunciado.

## What Changes

- Publicar en `settlements` un contrato JSON v1 determinístico, con key
  `numericOrderId`, únicamente para el resultado original `APPLIED` con snapshot
  FILLED. CANCELLED y las republicaciones `DUPLICATE` no emiten.
- Coordinar settlement y offset siguiente en una misma transacción Kafka del
  contenedor, después del commit JDBC explícito. Eliminar la confirmación
  independiente del listener y conservar procesamiento en serie por partición.
- Recuperar intención y datos desde el resultado SQL inmutable de F-03, sin outbox,
  flag `sent`, consulta al estado actual ni migración SQL para este contrato.
- Resolver abortos y commits inciertos sin reenvíos desde una sesión ambigua,
  usando offsets estables y metadatos del grupo para excluir consumidores obsoletos.
- Verificar con infraestructura real salida/offset, duplicados, caída de proceso,
  respuesta de commit perdida y transferencia entre instancias. Estas pruebas
  indispensables pertenecen a F-04; F-05 conserva escenarios adicionales de
  recuperación y operación.
- Activar la emisión al desplegar F-04 sobre F-03, incluyendo FILLED con resultado
  APPLIED y offset pendiente. No liquidar retroactivamente posiciones ya confirmadas.
  Documentar compatibilidad, límites y evidencia sin modificar ahora el estado
  pendiente de la feature.

## Capabilities

### New Capabilities

- `settlement-publication`: contrato de settlement, elegibilidad por resultado
  durable, publicación única observable con `read_committed`, recuperación y
  alcance de activación.

### Modified Capabilities

- `execution-report-consumption`: extender `Commit after persistence` para exigir
  la transacción Kafka conjunta de salida y offset, recuperación segura del commit
  incierto y conservación del registro pendiente ante errores.

## Impact

Afecta al listener y su factory personalizada, al adaptador Kafka de salida nuevo
y a la composición de un settlement desde `ProcessingResult`. Se mantienen el
dominio, la atomicidad JDBC, resultados existentes, contrato HTTP y particionado.
Se reutilizan Spring Kafka, PostgreSQL, imágenes fijadas y el topic ya creado; no
se agregan servicios ni dependencias de producción.

Se adaptará la inyección de fallas de `ExecutionReportKafkaIT`, actualmente basada
en `commitSync`, y se agregará evidencia con dos instancias e inspección independiente
de Kafka. README y DECISIONS se actualizarán durante implementación con evidencia
observada. Quedan fuera UI y nuevos escenarios de consola, consumidor downstream,
cálculos financieros, backfill histórico, cuarentena F-06, pérdida física de datos,
resets arbitrarios y despliegue mixto de consumidores F-03/F-04.
