# Spec Delta

## ADDED Requirements

### Requirement: Structured scenario evidence
Cada resultado de escenario SHALL exponer evidencia estructurada de sus envíos,
resultados SQL observados, snapshots de órdenes con ledger y salidas observadas.
SHALL conservar las identidades de ejecución, escenario, paso, orden y posición,
con fuente y fecha, sin interpretar títulos o conclusiones. La ampliación SHALL
ser aditiva y conservar campos, endpoints e identificadores existentes.

#### Scenario: Interleaved input and order correlation
- **WHEN** finaliza interleaved-orders
- **THEN** la respuesta permite relacionar sus siete envíos con posiciones, resultados y las dos órdenes con ledger completo
- **AND** las relaciones usan posiciones completas y ledgerId, sin depender del orden de propiedades JSON ni de textos descriptivos

#### Scenario: Historical duplicate results
- **WHEN** finaliza terminal-duplicates
- **THEN** se exponen los cuatro envíos, dos APPLIED y dos DUPLICATE, con dos filas de ledger
- **AND** NEW original y NEW republicado conservan snapshots distintos y referencia a la misma entrada de ledger
- **AND** IDs, offsets, contadores y cantidades conservan exactitud aunque excedan el rango seguro de JavaScript

#### Scenario: Quantity and restart observations
- **WHEN** se ejecutan quantity-snapshots o restart-persistence
- **THEN** se conservan las observaciones de estado y ledger por paso que permiten comparar 0/3/7/10 o el antes/después del reinicio, respectivamente
- **AND** una observación posterior no sobrescribe la anterior y una consulta actual se identifica separadamente

#### Scenario: Settlement and recovery evidence
- **WHEN** se ejecutan settlement-once o commit-boundary-recovery
- **THEN** la evidencia vincula salidas observadas con el resultado FILLED original y la posición fuente
- **AND** las fronteras, controles y recuperación conservan su asociación al caso y paso, sin atribuir autoría histórica por una asignación actual
- **AND** el alcance de la lectura read_committed permite distinguir salida pendiente, lectura parcial y conteo final verificado

#### Scenario: Quarantine evidence
- **WHEN** poison-recovery captura un parcial inválido y luego lo corrige
- **THEN** la evidencia distingue original, diagnóstico, aviso DLQ observado o pendiente de observación, corrección y ER efectivo aplicado
- **AND** la posición original conecta corrección, resultado y ledger, sin presentar el original ilegible como un ER válido
- **AND** los settlements y avisos de rechazo se presentan con su tipo y fuente correspondientes

### Requirement: Evidence capture lifecycle
La evidencia SHALL conservar observaciones parciales durante ejecución, fallo o
cancelación y quedar disponible con el historial en memoria mientras exista la
ejecución. Consultarla SHALL ser una lectura sin publicar, controlar servicios ni
modificar offsets. La disponibilidad de evidencia SHALL distinguirse del éxito
de las verificaciones y no provocar nuevas acciones al recargar la UI.

#### Scenario: Abort and missing SQL observation
- **WHEN** committed-input-only registra un envío abortado o una posición aún carece de observación SQL
- **THEN** se conserva el envío y su condición conocida sin inventar una aplicación o un error
- **AND** el envío abortado se identifica como evidencia del productor, no como lectura visible read_committed

#### Scenario: Batch and repeat isolation
- **WHEN** se consulta un lote de ocho escenarios o se repite uno con órdenes nuevas
- **THEN** cada conjunto de evidencia está limitado a su ejecución y escenario, con sus pasos y órdenes
- **AND** consultar un resultado anterior disponible no lo reemplaza con datos de otra corrida

#### Scenario: Failure and cancellation
- **WHEN** un paso falla o la ejecución se cancela después de emitir mensajes
- **THEN** los envíos y observaciones ya capturados siguen accesibles y las comprobaciones no completadas mantienen su estado real
- **AND** conservar evidencia no inicia próximos pasos ni evita la restauración existente

#### Scenario: Read after completion or console restart
- **WHEN** se consulta una ejecución terminada todavía disponible en memoria
- **THEN** la evidencia capturada conserva sus valores y fechas sin consultar nuevamente dependencias para reconstruirla
- **WHEN** la consola pierde ese historial al reiniciarse o por su límite de retención
- **THEN** se informa que la ejecución no está disponible, sin recrearla ni ejecutarla automáticamente
