# Spec Delta

## Purpose

Conservar el resultado de cada registro Kafka resuelto exitosamente para recuperar
qué ocurrió ante una reentrega, sin repetir efectos ni reconstruir el pasado desde
el estado vigente de la orden.

## ADDED Requirements

### Requirement: Durable result per input position
El sistema SHALL conservar un único resultado exitoso por `(topic, partition,
offset)`, compartido entre instancias y recuperable tras reinicio. El resultado
SHALL incluir `APPLIED` o `DUPLICATE`, `numericOrderId`, `fixId`, la referencia al
ledger del ER y un snapshot del estado, cantidades y contador al resolver por
primera vez esa posición.

#### Scenario: Newly applied report
- **WHEN** un ER nuevo válido se aplica desde una posición sin resultado
- **THEN** se conserva un resultado `APPLIED` con su entrada de ledger y el snapshot posterior a esa aplicación
- **AND** el caso de uso devuelve ese resultado tras confirmar SQL

#### Scenario: Business duplicate at another position
- **GIVEN** un ER ya aplicado y una orden que pudo avanzar desde esa aplicación
- **WHEN** el mismo ER llega en otra posición Kafka sin resultado previo
- **THEN** esa posición conserva y devuelve `DUPLICATE`, la referencia al ledger existente y el snapshot vigente al detectar el duplicado
- **AND** no agrega ledger ni modifica estado o contador

#### Scenario: Position identity includes topic and partition
- **WHEN** registros válidos tienen el mismo offset en topics o particiones diferentes
- **THEN** sus resultados se identifican por la tupla completa y no se sobrescriben

### Requirement: Immutable replay result
Una reentrega con la misma posición, key y contenido equivalente del ER SHALL
devolver el resultado persistido sin evaluar otra transición, escribir otro
resultado ni modificar estado, contador o ledger. SHALL conservar su clasificación,
referencia y snapshot originales aunque la orden haya avanzado.

#### Scenario: Replay of applied report after order advances
- **GIVEN** un `NEW` con resultado `APPLIED` y una orden que después llegó a `FILLED`
- **WHEN** se procesa otra vez la posición del `NEW`
- **THEN** se devuelve el mismo resultado `APPLIED`, con snapshot `NEW` y contador 1
- **AND** la orden sigue `FILLED` con su contador y ledger intactos

#### Scenario: Replay of duplicate result
- **GIVEN** una posición resuelta como `DUPLICATE` y transiciones posteriores de la orden
- **WHEN** se reentrega esa posición, incluso desde otra instancia o tras reiniciar
- **THEN** se devuelve el resultado `DUPLICATE` original, sin actualizar su snapshot

#### Scenario: Changed input at an existing position
- **GIVEN** una posición con resultado persistido
- **WHEN** se intenta procesar esa posición con otra key o distinto contenido JSON
- **THEN** se señaliza un conflicto y no se sobrescribe el resultado ni se modifica la orden

### Requirement: Atomic processing result and order effects
El resultado `APPLIED` SHALL confirmarse atómicamente con estado, incremento del
contador y ledger; `DUPLICATE` SHALL confirmarse sin repetir esos efectos. Una
falla de persistencia SHALL revertir todas las escrituras de ese intento. Sólo
resultados confirmados SHALL devolverse como procesamiento exitoso.

#### Scenario: Result write fails after order writes
- **WHEN** falla la escritura del resultado de un ER nuevo después de escribir su estado y ledger
- **THEN** no queda resultado ni cambio en estado, contador o ledger para ese intento
- **AND** reintentar sin la falla permite aplicar una sola vez el ER

#### Scenario: Ledger write fails
- **WHEN** falla la escritura del ledger de un ER nuevo
- **THEN** no quedan resultado ni actualización parcial de la orden

#### Scenario: Concurrent same-position delivery
- **WHEN** dos transacciones intentan procesar la misma posición y el mismo ER
- **THEN** ambas recuperan el mismo resultado y queda una sola fila de resultado y una sola aplicación

#### Scenario: Concurrent republications
- **WHEN** dos posiciones diferentes con el mismo ER se procesan concurrentemente
- **THEN** se conserva un resultado `APPLIED` y otro `DUPLICATE`, ambos referidos al mismo ledger
- **AND** el contador aumenta sólo una vez

### Requirement: Failed attempts remain pending
Un ER con JSON/key inválido, conflicto de identidad o transición inválida, o cuyo
intento SQL falla, SHALL permanecer pendiente con diagnóstico visible. No SHALL
crearse un resultado exitoso para ese intento ni alterarse un resultado anterior.
La ausencia de resultado SHALL significar ausencia de éxito registrado, sin
distinguir por sí sola entre registro no intentado e intento fallido.

#### Scenario: Invalid report or conflicting business identity
- **WHEN** falla la validación de entrada, transición o comparación del ER con el ledger
- **THEN** no se crea resultado para esa posición ni se modifican orden o ledger
- **AND** se conserva la política de detención y diagnóstico con topic, partición, offset y key

#### Scenario: Successful retry after rollback
- **GIVEN** un intento SQL revertido y sin resultado persistido
- **WHEN** se reintenta el mismo registro después de resolver la causa
- **THEN** el ER puede producir su primer resultado exitoso y una única aplicación
