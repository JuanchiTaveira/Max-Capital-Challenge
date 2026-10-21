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
El consumidor SHALL confirmar el progreso Kafka sólo después del commit SQL del
ER y su resultado, o de recuperar un resultado confirmado. SHALL confirmar el
offset siguiente y el settlement elegible en una misma transacción Kafka, sin
confirmación independiente del offset. Ante falla SHALL conservar el pendiente y
diagnóstico; ante commit incierto SHALL resolver el progreso estable antes de
reprocesar, sin avanzar al siguiente ER desde la sesión incierta.

#### Scenario: Database failure
- **WHEN** falla la transacción SQL al procesar un ER, incluida la escritura de su resultado
- **THEN** no se confirma su offset ni se observa una actualización parcial de la orden
- **AND** no se confirma settlement y el ER permanece disponible para reprocesamiento

#### Scenario: Invalid transition
- **WHEN** un ER válido en formato viola las reglas de transición de la orden
- **THEN** no se confirma su offset y el diagnóstico identifica la orden y la causa
- **AND** no se publica settlement confirmado

#### Scenario: SQL committed and offset confirmation fails
- **GIVEN** un resultado SQL confirmado para un registro Kafka
- **WHEN** falla su transacción Kafka antes de confirmar y el registro se reentrega al reanudar el consumidor
- **THEN** se recupera el mismo resultado sin repetir efectos SQL
- **AND** el offset se confirma junto al settlement elegible al completar la recuperación
- **AND** no se aplica el siguiente ER de la partición antes de resolver el pendiente

#### Scenario: Republished terminal duplicate
- **GIVEN** una orden terminal y su ER terminal ya aplicado
- **WHEN** el mismo ER se publica en otra posición Kafka
- **THEN** esa posición conserva un resultado `DUPLICATE` sin cambiar orden ni ledger
- **AND** se confirma su offset sólo después del commit de ese resultado, mediante una transacción Kafka sin settlement adicional

#### Scenario: Invalid input remains uncommitted
- **WHEN** falla JSON/key o el ER tiene una identidad aplicada con contenido distinto
- **THEN** no se registra un resultado exitoso para esa posición ni se confirma su offset
- **AND** se conserva el diagnóstico y se detiene el consumo de esa instancia sin saltar al siguiente registro
- **AND** reanudar repetidamente no recupera el registro mediante descarte silencioso

#### Scenario: Output and offset share commit
- **GIVEN** un FILLED elegible con resultado SQL confirmado
- **WHEN** se envía settlement y se incorpora el offset siguiente a una transacción Kafka abierta
- **THEN** antes de confirmar no avanza el offset estable ni se observa la salida con `read_committed`
- **AND** tras confirmar ambos quedan durables; abortar deja ambos sin confirmar

#### Scenario: Nonterminal progress is transactional
- **WHEN** un NEW o un parcial confirma su resultado SQL
- **THEN** su offset siguiente se confirma mediante la misma modalidad transaccional, sin settlement
- **AND** un aborto Kafka deja el registro pendiente y su reentrega recupera el resultado SQL original

#### Scenario: Uncertain commit stops the current session
- **WHEN** la aplicación no puede determinar si confirmó su transacción Kafka
- **THEN** deja diagnóstico con topic, partición, offset y causa, y detiene esa sesión antes de aplicar sucesores
- **AND** al reanudar obtiene offsets estables después de resolver transacciones pendientes
- **AND** una transacción confirmada no se reenvía y una abortada se recupera desde el resultado SQL

#### Scenario: Partition ownership is lost during processing
- **GIVEN** una instancia que confirmó SQL para una posición pero todavía no confirmó Kafka
- **WHEN** pierde la pertenencia vigente al grupo y otra instancia recibe la partición
- **THEN** el consumidor obsoleto no puede confirmar salida y offset como una segunda resolución válida
- **AND** el propietario vigente recupera el resultado SQL sin repetir efectos y preserva la secuencia

### Requirement: Committed input only
El consumidor SHALL aplicar únicamente ER visibles para un consumidor Kafka
`read_committed`.

#### Scenario: Aborted producer transaction
- **WHEN** un productor aborta una transacción que contiene un ER
- **THEN** ese ER no crea ni modifica una orden
