# execution-report-consumption Specification

## Purpose

Recibir ExecutionReports del formato descripto en el enunciado en la secuencia de
cada orden y conservar de forma visible los reportes que todavía no pueden aplicarse.

## Requirements

### Requirement: ExecutionReport input contract
El consumidor SHALL aceptar en `execution-reports` un JSON UTF-8 con `fixId` y
`numericOrderId` enteros positivos de 64 bits, `status` entre `NEW`,
`PARTIALLY_FILLED`, `FILLED` y `CANCELLED`, `nominalAmounts` positivo,
`leavesNominalAmount` y `accumulativeNominalAmount` no negativos, y
`transactionTime` ISO-8601 local u offset. Las cantidades SHALL admitir hasta
12 dígitos enteros y 8 decimales, sin redondeo. La key Kafka SHALL ser el
decimal canónico de `numericOrderId` del payload.

#### Scenario: Valid report with fields from the brief
- **WHEN** se publica un ER válido con los campos obligatorios y otros campos del enunciado, como `marketOrderId`, `ticker`, `executionPrice` o `secondaryTradeId`
- **THEN** se entrega al procesamiento y sus campos adicionales se conservan para el ledger

#### Scenario: Invalid envelope or key
- **WHEN** el JSON es inválido, falta un campo obligatorio o la key no coincide con `numericOrderId`
- **THEN** el ER no modifica la orden ni su ledger
- **AND** su offset no se confirma y el error queda visible para diagnóstico

### Requirement: Per-order sequence
El sistema SHALL aplicar los ER de un mismo `numericOrderId` en el orden de su
partición Kafka, sin procesar simultáneamente dos registros de esa partición.
`transactionTime` SHALL ser dato del ER, no criterio de reordenamiento.

#### Scenario: Interleaved orders
- **WHEN** el broker entrega ER de las órdenes A y B intercalados y dos instancias consumen el grupo
- **THEN** el ledger de cada orden conserva su propia secuencia de emisión

#### Scenario: Independent orders
- **WHEN** se publican ER válidos de órdenes asignadas a particiones distintas
- **THEN** las dos instancias pueden procesar esas particiones en paralelo sin alterar el orden interno de cada una

### Requirement: Commit after persistence
El consumidor SHALL confirmar el progreso Kafka sólo después de confirmar la
transacción SQL del ER y su resultado por `(topic, partition, offset)`, o de
recuperar un resultado ya confirmado para ese registro; ante una falla SHALL
conservar el registro pendiente y señalizar el error, sin descartarlo
silenciosamente.

#### Scenario: Database failure
- **WHEN** falla la transacción SQL al procesar un ER, incluida la escritura de su resultado
- **THEN** no se confirma su offset ni se observa una actualización parcial de la orden
- **AND** el ER permanece disponible para reprocesamiento

#### Scenario: Invalid transition
- **WHEN** un ER válido en formato viola las reglas de transición de la orden
- **THEN** no se confirma su offset y el diagnóstico identifica la orden y la causa

#### Scenario: SQL committed and offset confirmation fails
- **GIVEN** un resultado SQL confirmado para un registro Kafka
- **WHEN** falla la confirmación de su offset y el registro se reentrega al reanudar el consumidor
- **THEN** se recupera el mismo resultado sin repetir efectos SQL
- **AND** el offset puede confirmarse después de recuperar el resultado
- **AND** no se aplica el siguiente ER de la partición antes de resolver el pendiente

#### Scenario: Republished terminal duplicate
- **GIVEN** una orden terminal y su ER terminal ya aplicado
- **WHEN** el mismo ER se publica en otra posición Kafka
- **THEN** esa posición conserva un resultado `DUPLICATE` sin cambiar orden ni ledger
- **AND** se confirma su offset sólo después del commit de ese resultado

#### Scenario: Invalid input remains uncommitted
- **WHEN** falla JSON/key o el ER tiene una identidad aplicada con contenido distinto
- **THEN** no se registra un resultado exitoso para esa posición ni se confirma su offset
- **AND** se conserva el diagnóstico y se detiene el consumo de esa instancia sin saltar al siguiente registro

### Requirement: Committed input only
El consumidor SHALL aplicar únicamente ER visibles para un consumidor Kafka
`read_committed`.

#### Scenario: Aborted producer transaction
- **WHEN** un productor aborta una transacción que contiene un ER
- **THEN** ese ER no crea ni modifica una orden
