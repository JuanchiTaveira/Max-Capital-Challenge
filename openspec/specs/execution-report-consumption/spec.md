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

### Requirement: Recovery from interrupted SQL writes
Ante la terminación abrupta de un proceso con escrituras SQL sin confirmar, el
sistema SHALL recuperar la misma posición Kafka sin conservar efectos parciales.
El ER SHALL aplicarse una sola vez y sus sucesores SHALL conservar la secuencia
de la partición al continuar desde el propietario vigente.

#### Scenario: Process killed with an open SQL transaction
- **GIVEN** dos instancias activas y un ER suspendido después de sus escrituras SQL pero antes del commit, seguido por otros ER válidos en la misma partición
- **WHEN** se termina abruptamente el proceso suspendido y la otra instancia recibe su partición
- **THEN** las escrituras no confirmadas desaparecen y el offset del ER permanece pendiente hasta su recuperación
- **AND** el ER tiene una sola aplicación, una entrada de ledger y un resultado APPLIED al recuperarse
- **AND** el ledger, contador y cantidades finales reflejan exactamente la secuencia de ER publicados

#### Scenario: Initial order creation interrupted
- **GIVEN** un NEW que inicializó la orden y escribió sus efectos dentro de SQL sin confirmar
- **WHEN** el proceso termina abruptamente antes del commit y otro propietario recibe la posición
- **THEN** no queda una orden parcialmente creada y el NEW se aplica una sola vez antes de los ER posteriores

### Requirement: Partition reassignment during SQL processing
Si cambia el propietario durante una transacción SQL, el sistema SHALL resolver
el mismo ER sin duplicar efectos y preservar los resultados originales ya
confirmados. El propietario anterior SHALL ser incapaz de confirmar progreso
Kafka con pertenencia obsoleta; el vigente SHALL resolver el pendiente antes de
aplicar sus sucesores.

#### Scenario: Previous owner commits SQL after losing membership
- **GIVEN** un propietario suspendido con escrituras SQL abiertas y un segundo consumidor activo
- **WHEN** se observa la reasignación real de su partición, se libera el intento anterior y éste consigue confirmar SQL
- **THEN** el propietario vigente recupera el resultado original sin repetir estado, contador ni ledger
- **AND** el intento anterior no confirma progreso Kafka válido después de perder pertenencia
- **AND** tras finalizar ambos intentos, los sucesores conservan su secuencia y existe una sola resolución durable por posición

#### Scenario: Previous owner rolls back after losing membership
- **GIVEN** una transacción SQL abierta cuyo consumidor perdió la partición
- **WHEN** ese intento revierte y el propietario vigente reintenta la posición
- **THEN** se conserva una única aplicación exitosa y el flujo continúa sin saltar el ER revertido

### Requirement: Recovery after both application processes stop
Tras terminar abruptamente ambas instancias, el sistema SHALL reanudar desde
datos SQL y offsets estables del mismo grupo, conservando pendientes y resultados
confirmados. Al recuperar las dos instancias SHALL volver a repartir las dos
particiones y procesar nuevos ER sin pérdida ni doble aplicación.

#### Scenario: Both processes killed with pending work
- **GIVEN** ambas instancias activas, un ER con SQL confirmado y Kafka pendiente, y sucesores publicados sin resolver en su partición
- **WHEN** se terminan abruptamente ambas aplicaciones y se vuelven a iniciar conservando PostgreSQL, Kafka y el grupo
- **THEN** se recupera el mismo resultado SQL para el pendiente y se resuelven sus sucesores en secuencia
- **AND** las posiciones ya confirmadas no generan nuevos efectos
- **AND** se observan dos miembros con una partición cada uno y se procesan nuevos ER en ambas particiones

#### Scenario: Failed consumption session requires operational restart
- **WHEN** una falla detiene una sesión consumidora y se resuelve su causa
- **THEN** reiniciar la aplicación permite recuperar pendientes desde progreso estable
- **AND** el estado HTTP saludable por sí solo no se presenta como evidencia de consumo reanudado
