# Spec Delta

## MODIFIED Requirements

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
- **AND** queda en cuarentena con diagnóstico y su offset sólo avanza después de conservar original y bloqueo y confirmar su rechazo transaccional

### Requirement: Per-order sequence
El sistema SHALL aplicar los ER de un mismo `numericOrderId` en el orden de su
partición Kafka, sin procesar simultáneamente dos registros de esa partición.
`transactionTime` SHALL ser dato del ER, no criterio de reordenamiento. Una barrera durable SHALL impedir aplicar sucesores pendientes; capturarlos sin aplicarlos SHALL conservar la secuencia para recuperación.

#### Scenario: Interleaved orders
- **WHEN** el broker entrega ER de las órdenes A y B intercalados y dos instancias consumen el grupo
- **THEN** el ledger de cada orden conserva su propia secuencia de emisión

#### Scenario: Independent orders
- **WHEN** se publican ER válidos de órdenes asignadas a particiones distintas
- **THEN** las dos instancias pueden procesar esas particiones en paralelo sin alterar el orden interno de cada una

### Requirement: Commit after persistence
El consumidor SHALL confirmar el progreso Kafka sólo después del commit SQL del
ER y su resultado, o de recuperar un resultado confirmado. Para una captura diferida o rechazo SHALL requerir su evidencia y barrera durables; el rechazo y su progreso SHALL confirmar juntos en Kafka. SHALL confirmar el
offset siguiente y las salidas elegibles en una misma transacción Kafka, sin
confirmación independiente del offset. Ante falla SHALL conservar el pendiente y
diagnóstico; ante commit incierto SHALL resolver el progreso estable antes de
reprocesar, sin avanzar al siguiente ER desde la sesión incierta.

#### Scenario: Database failure
- **WHEN** falla la transacción SQL al procesar un ER, incluida la escritura de su resultado
- **THEN** no se confirma su offset ni se observa una actualización parcial de la orden
- **AND** no se confirma settlement y el ER permanece disponible para reprocesamiento

#### Scenario: Invalid transition
- **WHEN** un ER válido en formato viola las reglas de transición de la orden
- **THEN** no se aplica el ER y el diagnóstico de cuarentena identifica la orden y la causa
- **AND** su progreso sólo se confirma con rechazo y bloqueo durable según la política de cuarentena
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
- **THEN** no se registra un resultado exitoso para esa posición y su offset permanece sin confirmar hasta conservar original, diagnóstico y bloqueo durable y completar el rechazo transaccional
- **AND** rechazo y progreso se confirman juntos después del bloqueo durable; sus sucesores se retienen sin aplicar
- **AND** reanudar repetidamente no descarta el original ni libera el bloqueo sin resolución auditada

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

## ADDED Requirements

### Requirement: Recovery control input
La entrada SHALL distinguir ER ordinarios de comandos internos versionados de recuperación. Los comandos SHALL usar la partición original y requerir una solicitud durable válida. Un comando inválido SHALL conservar diagnóstico sin aplicar negocio ni bloquear indefinidamente el canal de resolución. No SHALL permitirse elegir una partición arbitraria desde el cliente.

#### Scenario: Valid recovery command behind retained reports
- **WHEN** el propietario recibe un comando para el primer pendiente después de capturar sucesores bloqueados
- **THEN** resuelve ese pendiente y confirma el progreso del comando junto a sus salidas elegibles
- **AND** los demás pendientes sólo se aplican mediante sus comandos posteriores

#### Scenario: Forged or stale command
- **WHEN** se recibe un comando sin solicitud registrada, con revisión incompatible o en partición distinta
- **THEN** no aplica ER ni libera barreras y conserva un diagnóstico de control inválido
- **AND** un control obsoleto no impide atender una resolución posterior válida
