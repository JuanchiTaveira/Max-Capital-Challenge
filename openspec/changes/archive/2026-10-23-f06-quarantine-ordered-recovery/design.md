# Design

## Context

Esta es una propuesta pendiente de implementación. F-04/F-05 ya resuelven estado, ledger y resultado en SQL, y luego settlement más offset en una transacción Kafka. Mantener pendiente la posición inválida permite reutilizar ese camino.

El enunciado prioriza un servicio chico, decisiones explícitas y pruebas de lo crítico. No exige aislamiento por orden dentro de una partición ni recuperación completamente automática de contenido incorrecto.

## Goals / Non-Goals

**Goals:** conservar errores visibles; distinguir transitorios y permanentes; ofrecer backoff exponencial con jitter; corregir con auditoría; mantener secuencia, idempotencia y settlement con dos instancias.

**Non-Goals:** continuar órdenes sanas de la misma partición bloqueada, persistir presupuestos de retry, automatizar la reparación humana, agregar comandos Kafka, outbox, workers, colas de pendientes SQL, UI nueva o hardening productivo.

## Decisions

### 1. El offset pendiente conserva la secuencia

Ante un error permanente se revierte el intento, se conserva el original, se notifica la DLQ y se pausa esa partición sin confirmar el offset. No se salta al siguiente ER ni se republican correcciones al final del topic.

La pausa debe mantener al consumidor haciendo polls y atendiendo las demás particiones asignadas. No se usa la detención de todo el listener para un error de datos: al retirarse del grupo trasladaría el problema a la otra instancia. Si hay rebalanceo, el nuevo propietario encuentra la cuarentena abierta y vuelve a pausar sólo esa partición.

El operador registra una corrección y reinicia la aplicación consumidora con los controles existentes. Reiniciar limpia la pausa local; el propietario que reciba la partición retoma desde su offset estable. No se agregan endpoints de coordinación entre instancias ni una tarea que consulte periódicamente SQL para reanudar.

```
Kafka: NEW -> parcial inválido -> FILLED
          aplicado     pausa      espera en Kafka
                           |
                  corrección + reinicio
                           |
                parcial efectivo -> FILLED
```

### 2. Dos tablas con responsabilidades claras

Una migración aditiva V3 agrega:

| Tabla | Contenido |
| --- | --- |
| `quarantined_reports` | Posición original única, key y valor originales en bytes, causa, fechas, estado OPEN/READY/RESOLVED, corrección seleccionada y fecha de notificación DLQ. |
| `report_corrections` | RequestId único, posición, key y JSON efectivos, motivo, responsable declarado, fecha y resultado ACCEPTED/APPLIED/DUPLICATE/REJECTED con causa si falla. |

Los originales y las correcciones anteriores no se sobrescriben. No se persisten los sucesores: permanecen en Kafka. No hay tabla de barreras, rondas, reservas de intentos, despachos ni resoluciones de comandos.

Se bloquea la fila de cuarentena al aceptar o aplicar una corrección y se reutiliza el lock de orden existente. No se introduce un lock global de partición. Marcar la corrección aplicada y la cuarentena resuelta comparte la transacción SQL de estado, contador, ledger y resultado; una falla revierte todo.

### 3. Retry acotado en memoria

Sólo se reintentan fallas explícitamente reconocidas cuyo intento SQL revirtió, como deadlock o serialización. JSON, campos, key, identidad y transición inválidos son permanentes. Una dependencia caída, una excepción desconocida, fencing o un commit Kafka incierto mantienen el offset pendiente con diagnóstico operativo; no se convierten en mensajes inválidos.

Valores iniciales: cinco intentos totales, espera base de 500 ms que se duplica, jitter uniforme entre 0 y 200 ms y espera máxima de 5 s. Se usan las facilidades de manejo de errores/backoff de Spring Kafka; no se incorpora una librería ni un motor propio de retry. La espera ocurre después del rollback, sin mantener transacciones SQL/Kafka abiertas y respetando `max.poll.interval.ms` de F-05.

Al agotar los intentos se pausa la partición y se registra el diagnóstico. El operador resuelve la causa y reinicia. El contador y la espera no sobreviven a reinicios o cambios de propietario: es un límite por sesión, no un presupuesto durable global. Se acepta expresamente esta limitación para el challenge.

### 4. DLQ como aviso, SQL como evidencia

Después del rollback se guarda la cuarentena con unicidad por topic/partición/offset. Luego se publica una notificación pequeña en `execution-reports-rejected`, en una transacción Kafka independiente que no incorpora el offset de entrada.

Contrato único: `schemaVersion`, `rejectionId` estable por posición, `source` y código de causa. La consulta por source devuelve el original SQL. No se duplica un payload potencialmente grande en DLQ; no hay modos INLINE/REFERENCE ni hashes.

Tras confirmar la notificación se guarda `dlq_notified_at` para evitar reenvíos ordinarios. Si el proceso cae entre Kafka y ese marcador SQL, o la respuesta del commit es incierta, la notificación puede repetirse con el mismo rejectionId. No se promete exactly-once para DLQ. Si falla captura o publicación, no avanza la entrada; al reanudar se completa el aviso faltante, también antes de aplicar una corrección.

Una corrección que vuelve a fallar agrega su diagnóstico a la auditoría y deja la cuarentena OPEN; no cambia el original ni genera otro rechazo inicial por esa posición. Las salidas y offsets de settlement siguen el protocolo de F-04/F-05.

### 5. Corrección local y reentrega de la misma posición

API propuesta en las aplicaciones:

- `GET /quarantine?topic=...&partition=...&offset=...`: original, causa, estado y correcciones. No se necesita un listado administrativo ni filtros adicionales.
- `POST /quarantine/corrections`: requestId, source, effectiveKey, effectiveReport, reason y declaredOperator.

POST valida formato e identidad/routing y registra metadata; devuelve 202 con la corrección ACCEPTED, no un éxito de negocio. Repetir requestId con el mismo contenido devuelve la misma corrección; reutilizarlo con otro contenido devuelve 409. Sólo se admite una corrección ACCEPTED por posición. No se puede corregir una posición con éxito previo. No se agrega expectedRevision: el lock de fila y esa regla bastan.

Si la key original era canónica, se conserva su numericOrderId. Ante key desconocida se exige una identidad explícita cuya key pertenezca a la partición original. No se mueve el mensaje entre particiones ni se modifica el particionado. Una asociación incompatible se rechaza; queda pendiente una decisión operativa fuera de este mecanismo.

Antes de parsear el registro, el listener busca si esa posición tiene una corrección. Si está READY utiliza su key y JSON efectivos; si está RESOLVED sigue utilizando la corrección aplicada para recuperar el mismo resultado después de una caída. Si está OPEN no intenta aplicar de nuevo el original ni avanza.

La aplicación conserva `(topic, partition, offset)` originales. El ledger guarda el ER efectivo; la cuarentena conserva los bytes recibidos y la auditoría explica la sustitución. Si confirma SQL y falla Kafka, la reentrega selecciona ese mismo ER efectivo y recupera el APPLIED/DUPLICATE existente. No se cambia el esquema de `processing_results` ni el contrato de settlement.

Si el ER efectivo viola una transición o identidad de negocio, el intento revierte; después se marca la corrección REJECTED y la cuarentena OPEN. El operador puede registrar otra corrección conservando el historial. Registrar una corrección nunca reanuda por sí solo ni escribe estado de orden.

### 6. Evidencia proporcional al alcance

Pruebas focalizadas: clasificación y límites del backoff; captura e idempotencia de correcciones; rollback conjunto; dos consumidores con la partición inválida pausada y otra activa; corrección seguida de sus sucesores; fallo de envío DLQ sin avance; y una caída después de SQL de una corrección FILLED con exactamente un settlement confirmado al recuperar. Se reutilizan los ensayos F-05 para las demás fronteras, sin duplicar toda su matriz.

`poison-recovery` usa órdenes nuevas: NEW, parcial ilegible y FILLED en una partición, más una orden sana en la otra. Observa la pausa, registra la corrección predefinida, reinicia con los controles existentes y compara contador, ledger y salida. Reutiliza el render y los modos automático/paso a paso existentes; no agrega HTML, formularios ni infraestructura de demo.

Al cancelar se procura corregir sólo el original propio ya publicado y restaurar el consumo. Si no se consigue, se informa la posición y el procedimiento manual; no se declara restauración exitosa. Si se pierde la consola, SQL y Kafka conservan el trabajo, pero la restauración del ensayo es manual. No se crea un registro durable adicional de escenarios.

## Risks / Trade-offs

- Una partición inválida demora todas sus órdenes hasta una corrección humana. Es la disponibilidad que se resigna para conservar el orden sin un subsistema de retención.
- Reiniciar renueva el límite local de retry. No hay contabilidad entre procesos ni protección contra reinicios operativos repetidos.
- La DLQ puede repetir notificaciones. Su ID permite correlacionarlas; no equivale a una segunda aplicación ni a otro settlement.
- La recuperación depende de conservar Kafka y PostgreSQL, el particionado estable y una corrección compatible. No hay descarte ni reasignación automática de identidades ambiguas.
- El responsable es declarado, sin autenticación. Las APIs pertenecen al entorno local del challenge.

## Migration Plan

1. Agregar V3 con las dos tablas y verificar que órdenes, ledger y resultados V2 no cambian.
2. Actualizar ambas apps conservando topics, grupo, offsets y volúmenes. No realizar backfill ni resetear posiciones.
3. Documentar consulta, corrección y reinicio con un ejemplo breve. Registrar las garantías observadas y estos límites en README/DECISIONS.
