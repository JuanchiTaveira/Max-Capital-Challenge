# Spec Delta

## MODIFIED Requirements

### Requirement: Settlement eligibility from durable results
El sistema SHALL emitir settlement sólo para una posición cuyo resultado SQL
original sea `APPLIED` con snapshot `FILLED`. SHALL recuperar esa decisión en una
reentrega sin reclasificar el resultado ni repetir efectos SQL. Un resultado
`DUPLICATE` SHALL producir cero settlements, incluso con snapshot `FILLED`.

#### Scenario: First completion
- **WHEN** un ER válido completa una orden y confirma su resultado `APPLIED` con snapshot `FILLED`
- **THEN** esa entrega origina un settlement en la transacción Kafka que confirma su progreso; durante recuperación la entrega es el comando y la fuente es el ER retenido
- **AND** el contador y ledger reflejan una sola aplicación del ER

#### Scenario: Pending original completion is redelivered
- **GIVEN** un FILLED con resultado `APPLIED` confirmado en SQL y transacción Kafka abortada
- **WHEN** se reentrega su misma posición a cualquiera de las dos instancias
- **THEN** se recupera la intención y los mismos datos del settlement
- **AND** al completar la recuperación queda un solo settlement confirmado sin otra aplicación SQL

#### Scenario: Republished terminal and late duplicate
- **GIVEN** una orden FILLED con settlement confirmado
- **WHEN** se republican su FILLED idéntico y un NEW idéntico en posiciones nuevas
- **THEN** ambas posiciones conservan `DUPLICATE` y no generan settlements adicionales
- **AND** el snapshot FILLED del duplicado tardío no se interpreta como una nueva finalización

### Requirement: Deterministic settlement contract
El settlement SHALL ser JSON UTF-8 en `settlements`, con key decimal canónica de
`numericOrderId`, `schemaVersion` 1, `settlementId` igual a `settlement:<numericOrderId>`,
`numericOrderId`, `fixId`, `ledgerId`, `source` con topic/partition/offset y cantidades
`nominalAmounts`, `accumulativeNominalAmount`, `leavesNominalAmount`. IDs y cantidades
SHALL ser cadenas decimales exactas; source.partition SHALL ser entero y source.offset
cadena decimal. Los datos SHALL provenir del resultado original. Para recuperación, source SHALL referenciar el ER retenido; la posición de entrega del comando SHALL conservarse en su resolución durable, sin cambiar el contrato v1.

#### Scenario: Output matches original completion
- **WHEN** se publica el settlement de un resultado APPLIED FILLED
- **THEN** identidad y cantidades coinciden con ese resultado SQL y source coincide con su posición fuente, que durante recuperación es la referencia al ER original retenido
- **AND** nominal y acumulado son iguales y el restante es cero
- **AND** el mensaje puede decodificarse con el contrato v1 y correlacionarse con su ledger

#### Scenario: Stable reconstruction with exact values
- **GIVEN** un resultado con identificadores mayores que el rango entero exacto de JavaScript y cantidades con ocho decimales
- **WHEN** se reconstruye el settlement después de cerrar y abrir la aplicación
- **THEN** conserva identidad, campos y representación canónica sin redondeos
- **AND** no incorpora timestamps ni identificadores aleatorios del nuevo intento

## ADDED Requirements

### Requirement: Transactional recovered settlement
Un FILLED aplicado por recuperación SHALL emitir settlement junto al offset del comando que produce su APPLIED original, después del commit SQL. Reentregar ese comando SHALL recuperar la intención; una copia en otra posición SHALL emitir cero salidas. Cuarentena y captura de pendientes SHALL emitir cero settlements.

#### Scenario: Original input offset already advanced
- **GIVEN** un FILLED retenido cuyo offset de entrada confirmó su captura sin aplicación
- **WHEN** un comando posterior lo aplica y confirma su progreso
- **THEN** se observa un solo settlement con source igual a la posición original del FILLED y ledgerId del ER efectivo
- **AND** el offset ya confirmado del original no se reutiliza como protección contra doble publicación

#### Scenario: Recovery process dies after SQL or acknowledged send
- **WHEN** el proceso cae después de aplicar FILLED en SQL o después de enviar settlement antes del commit Kafka
- **THEN** el propietario vigente recupera el resultado del mismo comando y completa su transacción
- **AND** después de resolver los intentos read_committed observa exactamente un settlement

#### Scenario: Recovery commit response is lost
- **WHEN** Kafka confirma settlement y offset del comando pero se pierde su respuesta
- **THEN** se detiene la sesión incierta y se recupera desde progreso estable sin republicar la finalización
- **AND** duplicar después la solicitud tampoco agrega salida ni ledger

#### Scenario: Recovered cancellation or equivalent correction
- **WHEN** la recuperación aplica CANCELLED o detecta un ER efectivo idéntico ya aplicado
- **THEN** no genera settlement y conserva respectivamente APPLIED CANCELLED o DUPLICATE
